# import os
# import requests
# import time
# import logging
# from pathlib import Path
# from dotenv import load_dotenv
# import json

# load_dotenv()
# SHOPIFY_DOMAIN = os.getenv('SHOPIFY_DOMAIN')
# ADMIN_API_TOKEN = os.getenv('DEMOATLANTIC_ADMIN_TOKEN')
# API_VERSION = os.getenv('SHOPIFY_API_VERSION', '2025-04')

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('bulk_operations.log'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)

# class ShopifyBulkOperator:
#     def __init__(self):
#         self.base_url = f"https://{SHOPIFY_DOMAIN}/admin/api/{API_VERSION}/graphql.json"
#         self.headers = {
#             'Content-Type': 'application/json',
#             'X-Shopify-Access-Token': ADMIN_API_TOKEN
#         }
#         self.polling_interval = 5  # Seconds between status checks

#     # Updated mutation queries according to Shopify's bulk operation requirements
#         self.mutations = {
#             'variant': """
#                 mutation bulkOperation($input: ProductVariantsBulkUpdateInput!) {
#                     productVariantsBulkUpdate(input: $input) {
#                         productVariants {
#                             id
#                             price
#                             compareAtPrice
#                         }
#                         userErrors {
#                             field
#                             message
#                         }
#                     }
#                 }
#             """,
#             'product': """
#                 mutation bulkOperation($input: ProductInput!) {
#                     productUpdate(input: $input) {
#                         product {
#                             id
#                         }
#                         userErrors {
#                             field
#                             message
#                         }
#                     }
#                 }
#             """,
#             'inventory': """
#                 mutation bulkOperation($input: InventorySetQuantitiesInput!) {
#                     inventorySetQuantities(input: $input) {
#                         inventoryAdjustmentGroup {
#                             id
#                         }
#                         userErrors {
#                             field
#                             message
#                         }
#                     }
#                 }
#             """
#         }

#     def _execute_graphql(self, query: str, variables: dict = None) -> dict:
#         """Execute GraphQL query with error handling"""
#         payload = {'query': query}
#         if variables:
#             payload['variables'] = variables

#         try:
#             response = requests.post(
#                 self.base_url,
#                 headers=self.headers,
#                 json=payload,
#                 timeout=30
#             )
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             logger.error(f"GraphQL request failed: {str(e)}")
#             if hasattr(e, 'response') and e.response:
#                 logger.error(f"Response content: {e.response.text}")
#             raise

#     def _staged_upload_create(self, filename: str) -> tuple:
#         """Create staged upload for JSONL file"""
#         query = """
#         mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
#             stagedUploadsCreate(input: $input) {
#                 stagedTargets {
#                     url
#                     parameters { name value }
#                 }
#                 userErrors { field message }
#             }
#         }
#         """
#         variables = {
#             "input": {
#                 "resource": "BULK_MUTATION_VARIABLES",
#                 "filename": filename,
#                 "mimeType": "text/jsonl",
#                 "httpMethod": "POST"
#             }
#         }

#         result = self._execute_graphql(query, variables)
#         if not result or 'data' not in result:
#             raise Exception("Failed to create staged upload")

#         targets = result['data']['stagedUploadsCreate']['stagedTargets']
#         if not targets:
#             raise Exception("No staged upload targets returned")

#         return targets[0]['url'], targets[0]['parameters']

#     def _upload_jsonl_file(self, file_path: str, upload_url: str, params: list) -> bool:
#         """Upload JSONL file to Shopify"""
#         try:
#             with open(file_path, 'rb') as f:
#                 files = {p['name']: (None, p['value']) for p in params}
#                 files['file'] = (Path(file_path).name, f.read(), 'text/jsonl')
                
#                 response = requests.post(
#                     upload_url,
#                     files=files,
#                     timeout=60
#                 )
#                 response.raise_for_status()
#                 return True
#         except Exception as e:
#             logger.error(f"File upload failed: {str(e)}")
#             raise

#     def _run_bulk_mutation(self, mutation: str, staged_path: str) -> str:
#         """Initiate bulk mutation operation with proper error handling"""
#         query = """
#         mutation bulkOperationRunMutation($mutation: String!, $stagedUploadPath: String!) {
#             bulkOperationRunMutation(
#                 mutation: $mutation,
#                 stagedUploadPath: $stagedUploadPath
#             ) {
#                 bulkOperation { id status }
#                 userErrors { field message }
#             }
#         }
#         """
#         variables = {
#             "mutation": mutation,
#             "stagedUploadPath": staged_path
#         }

#         result = self._execute_graphql(query, variables)
        
#         if not result:
#             raise Exception("Empty response from Shopify API")
            
#         if 'errors' in result:
#             error_messages = [e['message'] for e in result['errors']]
#             raise Exception(f"GraphQL errors: {', '.join(error_messages)}")
            
#         if 'data' not in result:
#             raise Exception("No data in API response")
            
#         operation_data = result['data']['bulkOperationRunMutation']
        
#         if operation_data.get('userErrors'):
#             errors = [e['message'] for e in operation_data['userErrors']]
#             raise Exception(f"Operation errors: {', '.join(errors)}")
            
#         if not operation_data.get('bulkOperation', {}).get('id'):
#             raise Exception("No operation ID returned. Check your mutation query format.")
            
#         return operation_data['bulkOperation']['id']

#     def _get_operation_status(self, operation_id: str) -> dict:
#         """Check bulk operation status"""
#         query = """
#         query bulkOperationStatus($id: ID!) {
#             node(id: $id) {
#                 ... on BulkOperation {
#                     id status errorCode
#                     createdAt completedAt
#                     objectCount fileSize url
#                 }
#             }
#         }
#         """
#         return self._execute_graphql(query, {"id": operation_id})

#     def _monitor_operation(self, operation_id: str) -> dict:
#         """Monitor operation progress until completion"""
#         logger.info(f"Monitoring operation {operation_id}")
#         last_count = 0
        
#         while True:
#             result = self._get_operation_status(operation_id)
#             if not result or 'data' not in result:
#                 raise Exception("Failed to get operation status")
            
#             op = result['data']['node']
#             current_count = int(op.get('objectCount', 0))
            
#             if current_count != last_count:
#                 logger.info(f"Processed {current_count:,} objects")
#                 last_count = current_count
            
#             if op['status'] in ['COMPLETED', 'FAILED', 'CANCELED']:
#                 logger.info(f"Operation {op['status']}")
#                 if op['status'] == 'COMPLETED' and op.get('url'):
#                     logger.info(f"Results available at: {op['url']}")
#                 return op
            
#             time.sleep(self.polling_interval)

#     def execute_bulk_update(self, operation_type: str) -> bool:
#         """Execute complete bulk update workflow"""
#         try:
#             # 1. Prepare file paths
#             jsonl_file = f"{operation_type}_updates.jsonl"
#             mutation_file = f"{operation_type}_mutation.graphql"
            
#             if not Path(jsonl_file).exists():
#                 raise FileNotFoundError(f"JSONL file not found: {jsonl_file}")
            
#             # 2. Use the predefined mutation query
#             mutation_query = self.mutations.get(operation_type)
#             if not mutation_query:
#                 raise ValueError(f"No mutation defined for operation type: {operation_type}")

#             # 3. Create staged upload
#             upload_url, params = self._staged_upload_create(Path(jsonl_file).name)
            
#             # 4. Extract staged upload path from parameters
#             staged_path = next((p['value'] for p in params if p['name'] == 'key'), None)
#             if not staged_path:
#                 raise Exception("Could not determine staged upload path")

#             # 5. Upload JSONL file
#             self._upload_jsonl_file(jsonl_file, upload_url, params)

#             # 6. Run bulk mutation
#             operation_id = self._run_bulk_mutation(mutation_query.strip(), staged_path)
            
#             # 7. Monitor operation
#             result = self._monitor_operation(operation_id)
            
#             return result['status'] == 'COMPLETED'
            
#         except Exception as e:
#             logger.error(f"Bulk operation failed: {str(e)}")
#             return False


# def main():
#     print("\nShopify Bulk Operation Runner")
#     print("=" * 40)
    
#     # # Verify environment variables
#     # if not all(os.getenv(var) for var in ['SHOPIFY_STORE_URL', 'SHOPIFY_ACCESS_TOKEN']):
#     #     print("Error: Missing required environment variables")
#     #     print("Please set SHOPIFY_STORE_URL and SHOPIFY_ACCESS_TOKEN in .env file")
#     #     return
    
#     operator = ShopifyBulkOperator()
    
#     # Operation selection
#     print("\nAvailable operations:")
#     operations = {
#         '1': 'variant',
#         '2': 'product', 
#         '3': 'inventory'
#     }
    
#     for num, op in operations.items():
#         print(f"{num}. {op.capitalize()} updates")
    
#     choice = input("\nSelect operation (1-3): ").strip()
#     if choice not in operations:
#         print("Invalid selection")
#         return
    
#     operation_type = operations[choice]
    
#     # Execute operation
#     print(f"\nStarting {operation_type} bulk operation...")
#     start_time = time.time()
    
#     success = operator.execute_bulk_update(operation_type)
    
#     duration = time.time() - start_time
#     mins, secs = divmod(duration, 60)
    
#     if success:
#         print(f"\n✅ {operation_type.capitalize()} bulk operation completed successfully!")
#     else:
#         print(f"\n❌ {operation_type.capitalize()} bulk operation failed")
    
#     print(f"Duration: {int(mins)}m {int(secs)}s")
#     print("Detailed logs saved to bulk_operations.log")


# if __name__ == "__main__":
#     import time
#     main()

from multiprocessing import Queue
from process import run_bulk_update_process

queue = Queue()
run_bulk_update_process(
    r"C:\Users\office\Desktop\Ashish\productDetailUpdate\bulkOperationRunMutation\test_CSV.csv",
    "output_results",
    queue,
    dry_run=False  # Set to True for testing
)