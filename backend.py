import pandas as pd
import json
import os
import requests
import time
import logging
from queue import Queue
from typing import Dict, List, Optional, Tuple
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

class ShopifyBulkMutationGenerator:
    """Generates JSONL files and GraphQL mutations for Shopify bulk operations."""
    
    def __init__(self, csv_filepath: str, location_id: str):
        self.csv_filepath = csv_filepath
        # Ensure location_id is a GID
        if not location_id.startswith("gid://shopify/Location/"):
            self.location_id = f"gid://shopify/Location/{location_id}"
        else:
            self.location_id = location_id
        self.df = self._load_csv()

    def _load_csv(self) -> pd.DataFrame:
        """Load and validate CSV file."""
        try:
            return pd.read_csv(self.csv_filepath)
        except Exception as e:
            logger.error(f"Error loading CSV: {str(e)}")
            raise

    def generate_mutation_queries(self):
        """Generate correct GraphQL mutation queries for bulk operations."""
        # Correct variant bulk mutation - uses ProductVariantInput
        variant_mutation = """
        mutation call($productId: ID!, $variants: [ProductVariantInput!]!) {
            productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                productVariants {
                    id
                    price
                    compareAtPrice
                    inventoryQuantity # Add to see updated quantity in response
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        
        # Correct product update mutation
        product_mutation = """
        mutation call($input: ProductInput!) {
            productUpdate(input: $input) {
                product {
                    id
                    metafields(first: 10) { # Fetch some metafields to confirm update
                        edges {
                            node {
                                key
                                value
                                namespace
                            }
                        }
                    }
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
            
        logger.info("Generated correct GraphQL mutation files")

    def process_csv(self) -> Tuple[bool, Dict]:
        """Process CSV and generate properly formatted JSONL files."""
        products_with_variants_to_update: Dict[str, Dict] = {} # Key: product_gid, Value: {"productId": ..., "variants": [...]}
        product_updates = [] # For product-level metafields
        
        processed_count = 0
        skipped_count = 0
        
        stats = {
            'total_rows': len(self.df),
            'skipped_rows': 0,
            'variant_updates_count': 0, # Total variants that will be attempted to update
            'product_metafield_updates_count': 0
        }

        for _, row in self.df.iterrows():
            processed_count += 1
            product_id_raw = row.get('ID')
            variant_id_raw = row.get('Variant ID')
            
            if pd.isna(product_id_raw) and pd.isna(variant_id_raw):
                logger.warning(f"Row {processed_count}: Missing Product ID and Variant ID. Skipping row.")
                stats['skipped_rows'] += 1
                continue

            # --- Process Variant Updates (Prices, Variant Metafields, Inventory) ---
            if pd.notna(variant_id_raw):
                product_gid = str(product_id_raw)
                if not product_gid.startswith("gid://shopify/Product/"):
                    product_gid = f"gid://shopify/Product/{product_gid}"

                variant_gid = str(variant_id_raw)
                if not variant_gid.startswith("gid://shopify/ProductVariant/"):
                    variant_gid = f"gid://shopify/ProductVariant/{variant_gid}"
                
                # Initialize product's variants list if not already present
                if product_gid not in products_with_variants_to_update:
                    products_with_variants_to_update[product_gid] = {
                        "productId": product_gid,
                        "variants": []
                    }
                
                variant_input_data = {"id": variant_gid}
                variant_metafields = []
                variant_data_added = False

                # Price / India
                price_value = row.get('Price / India')
                if pd.notna(price_value):
                    try:
                        variant_input_data["price"] = str(float(price_value))
                        variant_data_added = True
                    except (ValueError, TypeError):
                        logger.warning(f"Row {processed_count}: Invalid 'Price / India' value '{price_value}' for variant {variant_gid}.")

                # Compare At Price / India
                compare_at_price_value = row.get('Compare At Price / India')
                if pd.notna(compare_at_price_value):
                    try:
                        variant_input_data["compareAtPrice"] = str(float(compare_at_price_value))
                        variant_data_added = True
                    except (ValueError, TypeError):
                        logger.warning(f"Row {processed_count}: Invalid 'Compare At Price / India' value '{compare_at_price_value}' for variant {variant_gid}.")
                
                # Variant Metafield: custom.original_price [number_decimal]
                original_price_mf = row.get('Variant Metafield: custom.original_price [number_decimal]')
                if pd.notna(original_price_mf):
                    try:
                        variant_metafields.append({
                            "namespace": "custom",
                            "key": "original_price",
                            "type": "number_decimal",
                            "value": str(float(original_price_mf))
                        })
                        variant_data_added = True
                    except (ValueError, TypeError):
                        logger.warning(f"Row {processed_count}: Invalid 'Variant Metafield: custom.original_price' value '{original_price_mf}' for variant {variant_gid}.")
                
                # Variant Metafield: custom.original_currency [single_line_text_field]
                original_currency_mf = row.get('Variant Metafield: custom.original_currency [single_line_text_field]')
                if pd.notna(original_currency_mf):
                    variant_metafields.append({
                        "namespace": "custom",
                        "key": "original_currency",
                        "type": "single_line_text_field",
                        "value": str(original_currency_mf)
                    })
                    variant_data_added = True
                
                if variant_metafields:
                    variant_input_data["metafields"] = variant_metafields

                # Inventory Available: Head Office - DG
                inventory_available = row.get('Inventory Available: Head Office - DG')
                if pd.notna(inventory_available):
                    try:
                        inventory_quantity_value = int(float(inventory_available)) # Use float then int to handle potential '20.0'
                        if inventory_quantity_value >= 0: # Ensure non-negative quantity
                            variant_input_data["inventoryQuantities"] = [{
                                "quantity": inventory_quantity_value,
                                "locationId": self.location_id
                            }]
                            variant_data_added = True
                        else:
                            logger.warning(f"Row {processed_count}: Negative inventory quantity '{inventory_available}' for variant {variant_gid}. Skipping inventory update for this variant.")
                    except (ValueError, TypeError):
                        logger.warning(f"Row {processed_count}: Invalid 'Inventory Available' value '{inventory_available}' for variant {variant_gid}. Skipping inventory update for this variant.")

                # Only add variant to list if it has actual updates
                if variant_data_added or len(variant_input_data) > 1: # if more than just 'id' is present or specific data was added
                    products_with_variants_to_update[product_gid]["variants"].append(variant_input_data)
                    stats['variant_updates_count'] += 1
                else:
                    logger.info(f"Row {processed_count}: Variant {variant_gid} has no meaningful update data. Skipping variant for bulk update.")
                    stats['skipped_rows'] += 1

            # --- Process Product Updates (Product Metafields) ---
            if pd.notna(product_id_raw):
                product_gid_for_product_update = str(product_id_raw)
                if not product_gid_for_product_update.startswith("gid://shopify/Product/"):
                    product_gid_for_product_update = f"gid://shopify/Product/{product_gid_for_product_update}"

                product_metafields = []
                product_data_added = False

                # Metafield: custom.sla [single_line_text_field]
                sla_mf = row.get('Metafield: custom.sla [single_line_text_field]')
                if pd.notna(sla_mf):
                    product_metafields.append({
                        "namespace": "custom",
                        "key": "sla",
                        "type": "single_line_text_field",
                        "value": str(sla_mf)
                    })
                    product_data_added = True
                
                # Metafield: custom.vendor_source [single_line_text_field]
                vendor_source_mf = row.get('Metafield: custom.vendor_source [single_line_text_field]')
                if pd.notna(vendor_source_mf):
                    product_metafields.append({
                        "namespace": "custom",
                        "key": "vendor_source",
                        "type": "single_line_text_field",
                        "value": str(vendor_source_mf)
                    })
                    product_data_added = True
                
                if product_metafields:
                    product_updates.append({
                        "input": {
                            "id": product_gid_for_product_update,
                            "metafields": product_metafields
                        }
                    })
                    stats['product_metafield_updates_count'] += 1

        # Final filter for variant updates: ensure products actually have variants to update
        final_product_variant_groups = []
        for product_gid, product_data in products_with_variants_to_update.items():
            if product_data["variants"]: # Only include if there are variants to update for this product
                final_product_variant_groups.append(product_data)

        # Write JSONL files
        try:
            with open("variant_updates.jsonl", "w") as f:
                for item in final_product_variant_groups:
                    f.write(json.dumps(item) + "\n")
            
            with open("product_updates.jsonl", "w") as f:
                for item in product_updates:
                    f.write(json.dumps(item) + "\n")
            
            logger.info("Successfully generated JSONL files with correct format")
            return True, stats
            
        except Exception as e:
            logger.error(f"Error writing JSONL files: {str(e)}")
            return False, stats

class ShopifyBulkOperator:
    """Handles Shopify bulk operations execution with corrected implementation."""
    
    def __init__(self, shopify_domain: str, admin_api_token: str, api_version: str):
        self.base_url = f"https://{shopify_domain}/admin/api/{api_version}/graphql.json"
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': admin_api_token
        }
        self.polling_interval = 5  # seconds

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
            data = response.json()
            if 'errors' in data:
                logger.error(f"GraphQL errors from Shopify API: {json.dumps(data['errors'], indent=2)}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"GraphQL request failed: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response content: {e.response.text}")
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
        if not result or 'data' not in result or not result['data'].get('stagedUploadsCreate'):
            if result and 'errors' in result:
                raise Exception(f"Failed to create staged upload: {json.dumps(result['errors'])}")
            raise Exception("Failed to create staged upload: No data or stagedUploadsCreate found in response.")

        staged_uploads_data = result['data']['stagedUploadsCreate']
        if staged_uploads_data.get('userErrors'):
            raise Exception(f"User errors creating staged upload: {json.dumps(staged_uploads_data['userErrors'])}")

        targets = staged_uploads_data.get('stagedTargets')
        if not targets:
            raise Exception("No staged upload targets returned")

        return targets[0]['url'], targets[0]['parameters']

    def _upload_jsonl_file(self, file_path: str, upload_url: str, params: list) -> bool:
        """Upload JSONL file to Shopify."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"JSONL file not found at {file_path}")

        # Prepare form data from parameters
        files = {'file': open(file_path, 'rb')}
        data = {param['name']: param['value'] for param in params if param['name'] != 'file'}

        try:
            response = requests.post(upload_url, data=data, files=files)
            response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
            logger.info(f"Successfully uploaded {os.path.basename(file_path)} to staged location.")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error uploading JSONL file {os.path.basename(file_path)}: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Upload response content: {e.response.text}")
            raise # Re-raise the exception to be caught by the calling function

    def _run_bulk_mutation(self, mutation: str, staged_path: str) -> str:
        """Initiate bulk mutation operation with corrected implementation."""
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
        logger.debug(f"Raw API response from runMutation: {json.dumps(result, indent=2)}")
        if not result or 'data' not in result or not result['data'].get('bulkOperationRunMutation'):
            if result and 'errors' in result:
                raise Exception(f"Failed to initiate bulk mutation: {json.dumps(result['errors'])}")
            raise Exception("Failed to initiate bulk mutation: No data or bulkOperationRunMutation found in response.")

        op_data = result['data']['bulkOperationRunMutation']
        if op_data.get('userErrors'):
            raise Exception(f"User errors running bulk mutation: {json.dumps(op_data['userErrors'])}")

        bulk_op = op_data.get('bulkOperation')
        if bulk_op and bulk_op.get('id'):
            return bulk_op['id']
        raise Exception("No operation ID returned from bulkOperationRunMutation.")

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
        if not result or 'data' not in result or not result['data'].get('node'):
            if result and 'errors' in result:
                raise Exception(f"Failed to get operation status: {json.dumps(result['errors'])}")
            raise Exception("Failed to get operation status: No node found in response.")
        
        node = result['data']['node']
        if not node: # This can happen if the ID is invalid or operation not found
             raise Exception(f"Bulk operation with ID {operation_id} not found.")
        
        return node

    def _monitor_operation(self, operation_id: str, update_queue: Optional[Queue] = None) -> dict:
        """Monitor operation progress until completion."""
        logger.info(f"Monitoring operation {operation_id}")
        last_count = 0
        
        while True:
            status = self._get_operation_status(operation_id)
            current_count = int(status.get('objectCount', 0))
            
            if update_queue:
                update_queue.put({
                    "type": "progress",
                    "value": None, # Will be set by app.py based on overall progress
                    "text": f"Processing: {status['status']} ({current_count} objects)"
                })
                update_queue.put({"type": "log", "message": f"Operation {operation_id[:8]}... Status: {status['status']}, Processed: {current_count} objects."})
            
            if current_count != last_count:
                logger.info(f"Processed {current_count} objects")
                last_count = current_count
            
            if status['status'] in ['COMPLETED', 'FAILED', 'CANCELED']:
                logger.info(f"Operation {status['status']}")
                return status
            
            time.sleep(self.polling_interval)

    def download_results(self, result_url: str, output_filepath: str) -> bool:
        """Download bulk operation results."""
        if not result_url:
            logger.warning("No result URL provided to download.")
            return False
        
        logger.info(f"Downloading results from: {result_url}")
        try:
            response = requests.get(result_url, stream=True)
            response.raise_for_status()

            with open(output_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Successfully downloaded results to: {output_filepath}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading results from {result_url}: {e}")
            return False

    def process_result_file(self, result_filepath: str) -> Tuple[int, int, List[Dict]]:
        """Parse result JSONL file."""
        success_count = 0
        failure_count = 0
        errors = []

        if not os.path.exists(result_filepath):
            logger.warning(f"Result file not found: {result_filepath}")
            return 0, 0, []

        logger.info(f"Processing result file: {result_filepath}")
        with open(result_filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line)
                    if 'userErrors' in data and data['userErrors']:
                        failure_count += 1
                        errors.append(data)
                    elif 'errors' in data: # Top-level GraphQL errors for the batch
                        failure_count += 1
                        errors.append(data)
                    else:
                        success_count += 1
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decoding error in result file line {line_num}: {e} - Line: {line.strip()}")
                    failure_count += 1
                except Exception as e:
                    logger.error(f"Unexpected error processing result file line {line_num}: {e} - Line: {line.strip()}")
                    failure_count += 1
        
        logger.info(f"Result file processed: Successes={success_count}, Failures={failure_count}")
        return success_count, failure_count, errors