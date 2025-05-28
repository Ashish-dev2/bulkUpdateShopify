import pandas as pd
import json
import os
import requests
import time
import logging
from queue import Queue
from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

class ShopifyBulkMutationGenerator:
    """Generates JSONL files and GraphQL mutations for Shopify bulk operations."""
    
    def __init__(self, csv_filepath: str, location_id: str):
        self.csv_filepath = csv_filepath
        self.location_id = location_id
        self.df = self._load_csv()
        self.inventory_changes = []

    def _load_csv(self) -> pd.DataFrame:
        """Load and validate CSV file."""
        try:
            df = pd.read_csv(self.csv_filepath)
            
            # Validate required columns
            required_cols = {'ID', 'Variant ID', 'Inventory Item ID', 'Inventory Available: Head Office - DG'}
            missing = required_cols - set(df.columns)
            if missing:
                raise ValueError(f"Missing required columns: {missing}")
            
            return df
        except Exception as e:
            logger.error(f"Error loading CSV: {str(e)}")
            raise

    def generate_mutation_queries(self):
        """Generate GraphQL mutation queries for bulk operations."""
        # Existing variant and product mutations remain the same
        variant_mutation = """
        mutation call($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
            productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                productVariants {
                    id
                    price
                    compareAtPrice
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        
        product_mutation = """
        mutation call($input: ProductInput!) {
            productUpdate(input: $input) {
                product {
                    id
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """

        with open("variant_mutation.graphql", "w") as f:
            f.write(variant_mutation)
        
        with open("product_mutation.graphql", "w") as f:
            f.write(product_mutation)
            
        logger.info("Generated GraphQL mutation files")

    def process_csv(self) -> Tuple[bool, Dict]:
        """Process CSV and generate data for mutations."""
        variant_updates = []
        product_updates = []
        inventory_changes = []
        
        stats = {
            'total_rows': len(self.df),
            'skipped_rows': 0,
            'variant_updates': 0,
            'product_updates': 0,
            'inventory_updates': 0,
            'inventory_errors': 0
        }

        for _, row in self.df.iterrows():
            product_id = row.get('ID')
            variant_id = row.get('Variant ID')
            
            # Process variant updates
            if variant_id and product_id:
                variant_data = {
                    "id": f"gid://shopify/ProductVariant/{variant_id}",
                    "price": str(row.get('Price / India', '')) or None,
                    "compareAtPrice": str(row.get('Compare At Price / India', '')) or None,
                    "metafields": []
                }
                
                if pd.notna(row.get('Variant Metafield: custom.original_price [number_decimal]')):
                    variant_data["metafields"].append({
                        "namespace": "custom",
                        "key": "original_price",
                        "type": "single_line_text_field",
                        "value": str(row['Variant Metafield: custom.original_price [number_decimal]'])
                    })
                
                if pd.notna(row.get('Variant Metafield: custom.original_currency [single_line_text_field]')):
                    variant_data["metafields"].append({
                        "namespace": "custom",
                        "key": "original_currency",
                        "type": "single_line_text_field",
                        "value": str(row['Variant Metafield: custom.original_currency [single_line_text_field]'])
                    })
                
                if any(variant_data.values()):
                    variant_updates.append({
                        "productId": f"gid://shopify/Product/{product_id}",
                        "variants": [variant_data]
                    })
                    stats['variant_updates'] += 1

            # Process product updates
            if product_id:
                metafields = []
                if pd.notna(row.get('Metafield: custom.sla [single_line_text_field]')):
                    metafields.append({
                        "namespace": "custom",
                        "key": "sladay",
                        "type": "single_line_text_field",
                        "value": str(row['Metafield: custom.sla [single_line_text_field]'])
                    })
                
                if pd.notna(row.get('Metafield: custom.vendor_source [single_line_text_field]')):
                    metafields.append({
                        "namespace": "custom",
                        "key": "vendor_source",
                        "type": "single_line_text_field",
                        "value": str(row['Metafield: custom.vendor_source [single_line_text_field]'])
                    })
                
                if metafields:
                    product_updates.append({
                        "input": {
                            "id": f"gid://shopify/Product/{product_id}",
                            "metafields": metafields
                        }
                    })
                    stats['product_updates'] += 1

            # Process inventory changes
            try:
                inventory_item_id = row['Inventory Item ID']
                #existing_qty = row['Existing Quantity']
                qty = row['Inventory Available: Head Office - DG']
                
                if pd.notna(inventory_item_id) and pd.notna(qty):
                    #delta = int(float(new_qty)) - int(float(existing_qty))
                    #if delta != 0:
                        inventory_changes.append({
                            "inventoryItemId": f"gid://shopify/InventoryItem/{inventory_item_id}",
                            "locationId": self.location_id,
                            "quantity": int(float(qty))
                        })
                        stats['inventory_updates'] += 1
            except Exception as e:
                stats['inventory_errors'] += 1
                logger.warning(f"Inventory calculation error: {str(e)}")

        # Save data for bulk operations
        try:
            with open("variant_updates.jsonl", "w") as f:
                for item in variant_updates:
                    f.write(json.dumps(item) + "\n")
            
            with open("product_updates.jsonl", "w") as f:
                for item in product_updates:
                    f.write(json.dumps(item) + "\n")
            
            self.inventory_changes = inventory_changes
            logger.info("Processed CSV data successfully")
            return True, stats
            
        except Exception as e:
            logger.error(f"Error writing files: {str(e)}")
            return False, stats

class ShopifyBulkOperator:
    """Handles Shopify bulk operations execution."""

    RATE_LIMIT_CONFIG = {
        'max_points': 20000,         # Shopify Plus: 20000 & Standard:2000
        'refill_rate': 1000,         # Points per second, Shopify Plus: 1000 & Standard:100
        'request_cost': 15,          # Average cost per GraphQL request
        'burst_buffer': 0.9,         # 90% of max points as safety buffer
    }
    
    def __init__(self, shopify_domain: str, admin_api_token: str, api_version: str):
        self.base_url = f"https://{shopify_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': admin_api_token
        }
        self.polling_interval = 5  # seconds
        self.rate_limit_points = self.RATE_LIMIT_CONFIG['max_points']
        self.last_refill_time = time.time()

    def _calculate_refill(self):
        """Update available points based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill_time
        refilled = elapsed * self.RATE_LIMIT_CONFIG['refill_rate']
        
        self.rate_limit_points = min(
            self.RATE_LIMIT_CONFIG['max_points'],
            self.rate_limit_points + refilled
        )
        self.last_refill_time = now

    def _check_rate_limit(self):
        """Ensure we stay within rate limits, sleep if needed."""
        self._calculate_refill()
        
        required = self.RATE_LIMIT_CONFIG['request_cost']
        safety_threshold = self.RATE_LIMIT_CONFIG['max_points'] * self.RATE_LIMIT_CONFIG['burst_buffer']
        
        while self.rate_limit_points < required:
            deficit = required - self.rate_limit_points
            sleep_time = deficit / self.RATE_LIMIT_CONFIG['refill_rate']
            
            logger.debug(f"Rate limit exceeded, sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
            self._calculate_refill()

        self.rate_limit_points -= required
        logger.debug(f"Points remaining: {self.rate_limit_points:.2f}")


    def _execute_graphql(self, query: str, variables: dict = None) -> dict:
        """Execute GraphQL query with error handling."""
        payload = {'query': query}
        if variables:
            payload['variables'] = variables

        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"GraphQL request failed: {str(e)}")
            raise

    def _staged_upload_create(self, filename: str) -> Tuple[str, List[Dict]]:
        """Create staged upload for bulk operation files."""
        query = """
        mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
            stagedUploadsCreate(input: $input) {
                stagedTargets {
                    url
                    parameters { name value }
                }
                userErrors { field message }
            }
        }
        """
        variables = {
            "input": [{
                "resource": "BULK_MUTATION_VARIABLES",
                "filename": filename,
                "mimeType": "text/jsonl",
                "httpMethod": "POST"
            }]
        }

        result = self._execute_graphql(query, variables)
        if not result or 'errors' in result:
            raise Exception(f"Failed to create staged upload: {json.dumps(result.get('errors', 'Unknown error'))}")

        targets = result['data']['stagedUploadsCreate']['stagedTargets']
        if not targets:
            raise Exception("No staged upload targets returned")

        return targets[0]['url'], targets[0]['parameters']

    def _upload_jsonl_file(self, file_path: str, upload_url: str, params: list) -> bool:
        """Upload JSONL file to Shopify."""
        try:
            with open(file_path, 'rb') as f:
                files = {p['name']: (None, p['value']) for p in params}
                files['file'] = (Path(file_path).name, f.read(), 'text/jsonl')
                
                response = requests.post(
                    upload_url,
                    files=files,
                    timeout=60
                )
                response.raise_for_status()
                return True
        except Exception as e:
            logger.error(f"File upload failed: {str(e)}")
            raise

    def _run_bulk_mutation(self, mutation: str, staged_path: str) -> str:
        """Initiate bulk mutation operation."""
        query = """
        mutation bulkOperationRunMutation($mutation: String!, $stagedUploadPath: String!) {
            bulkOperationRunMutation(
                mutation: $mutation,
                stagedUploadPath: $stagedUploadPath
            ) {
                bulkOperation { id status }
                userErrors { field message }
            }
        }
        """
        variables = {
            "mutation": mutation,
            "stagedUploadPath": staged_path
        }

        result = self._execute_graphql(query, variables)
        if not result or 'errors' in result:
            raise Exception(f"Failed to initiate bulk mutation: {json.dumps(result.get('errors'))}")

        operation_data = result['data']['bulkOperationRunMutation']
        if operation_data.get('userErrors'):
            raise Exception(f"Operation errors: {json.dumps(operation_data['userErrors'])}")

        if not operation_data.get('bulkOperation', {}).get('id'):
            raise Exception("No operation ID returned")

        return operation_data['bulkOperation']['id']

    def _get_operation_status(self, operation_id: str) -> dict:
        """Check bulk operation status."""
        query = """
        query bulkOperationStatus($id: ID!) {
            node(id: $id) {
                ... on BulkOperation {
                    id status errorCode
                    createdAt completedAt
                    objectCount fileSize url
                }
            }
        }
        """
        result = self._execute_graphql(query, {"id": operation_id})
        if not result or 'errors' in result:
            raise Exception(f"Failed to get operation status: {json.dumps(result.get('errors'))}")

        return result['data']['node']

    def _monitor_operation(self, operation_id: str, update_queue: Queue = None) -> dict:
        """Monitor operation progress until completion."""
        last_count = 0
        
        while True:
            status = self._get_operation_status(operation_id)
            current_count = int(status.get('objectCount', 0))
            
            if update_queue:
                update_queue.put({
                    "type": "progress",
                    "text": f"Processed {current_count} objects"
                })
            
            if current_count != last_count:
                logger.info(f"Processed {current_count} objects")
                last_count = current_count
            
            if status['status'] in ['COMPLETED', 'FAILED', 'CANCELED']:
                logger.info(f"Operation {status['status']}")
                return status
            
            time.sleep(self.polling_interval)

    def download_results(self, result_url: str, output_filepath: str) -> bool:
        """Download bulk operation results."""
        try:
            response = requests.get(result_url, stream=True)
            response.raise_for_status()

            with open(output_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            logger.error(f"Error downloading results: {str(e)}")
            return False

    def process_result_file(self, result_filepath: str) -> Tuple[int, int, List[Dict]]:
        """Parse result JSONL file."""
        success_count = 0
        failure_count = 0
        errors = []

        with open(result_filepath, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if data.get('userErrors') or data.get('errors'):
                        failure_count += 1
                        errors.append(data)
                    else:
                        success_count += 1
                except json.JSONDecodeError:
                    failure_count += 1

        return success_count, failure_count, errors

class ShopifyInventoryUpdater(ShopifyBulkOperator):
    """Handles inventory quantity updates using batched GraphQL mutations."""
    
    MAX_CHANGES_PER_MUTATION = 250  # Shopify's limit
    REQUEST_COST = 15
    #RATE_LIMIT_DELAY = 1  # Seconds between requests
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.RATE_LIMIT_CONFIG['request_cost'] = self.REQUEST_COST
        self.mutation_query = """
        mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
            inventorySetQuantities(input: $input) {
                inventoryAdjustmentGroup {
                    createdAt
                    reason
                    referenceDocumentUri
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """

    def process_inventory_updates(self, changes: List[Dict], update_queue: Queue = None) -> Dict:
        """Process inventory updates in batches with error handling."""
        results = {
            'total_changes': len(changes),
            'batches_sent': 0,
            'successful_updates': 0,
            'failed_updates': 0,
            'errors': []
        }
        
        for batch_idx in range(0, len(changes), self.MAX_CHANGES_PER_MUTATION):
            batch = changes[batch_idx:batch_idx + self.MAX_CHANGES_PER_MUTATION]
            try: 
                # Check and wait for rate limit
                self._check_rate_limit()
                variables = {
                    "input": {
                        "reason": "correction",
                        "name": "available",
                        "ignoreCompareQuantity": True,
                        "quantities": batch
                    }
                }
            
                response = self._execute_graphql(self.mutation_query, variables)
                results['batches_sent'] += 1
                
                # Process response
                if response.get('errors'):
                    results['errors'].extend(response['errors'])
                    results['failed_updates'] += len(batch)
                    logger.error(f"Inventory batch {batch_idx//self.MAX_CHANGES_PER_MUTATION} failed with errors")
                else:
                    user_errors = response['data']['inventorySetQuantities']['userErrors']
                    if user_errors:
                        results['errors'].extend(user_errors)
                        results['failed_updates'] += len(user_errors)
                        results['successful_updates'] += (len(batch) - len(user_errors))
                        logger.warning(f"Inventory batch had {len(user_errors)} errors")
                    else:
                        results['successful_updates'] += len(batch)
                        logger.info(f"Successfully processed inventory batch of {len(batch)} items")
                
                # progress reporting
                if update_queue:
                    update_queue.put({
                        "type": "progress",
                        "text": f"Processed {batch_idx + len(batch)}/{len(changes)} inventory items"
                    })
                
            except Exception as e:
                logger.error(f"Inventory batch {batch_idx//self.MAX_CHANGES_PER_MUTATION} failed: {str(e)}")
                results['failed_updates'] += len(batch)
                results['errors'].append({
                    "batch": batch_idx // self.MAX_CHANGES_PER_MUTATION,
                    "error": str(e)
                })
            
            # Respect rate limits
            #time.sleep(self.RATE_LIMIT_DELAY)
        
        return results