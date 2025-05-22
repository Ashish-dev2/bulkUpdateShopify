import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import time
from multiprocessing import Queue
from backend import ShopifyBulkMutationGenerator, ShopifyBulkOperator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bulk_update_process.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_bulk_update_process(csv_filepath: str, output_results_dir: str, update_queue: Queue, dry_run: bool = False):
    """Main function to run the bulk update process."""
    start_time = time.time()
    
    # Load environment variables
    load_dotenv()
    shopify_domain = os.getenv("SHOPIFY_DOMAIN")
    admin_api_token = os.getenv("DEMOATLANTIC_ADMIN_TOKEN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-04")
    location_id = os.getenv("SHOPIFY_LOCATION_ID")

    # Validate configuration
    if not all([shopify_domain, admin_api_token, location_id]):
        error_msg = "Missing required environment variables"
        update_queue.put({"type": "error", "message": error_msg})
        logger.error(error_msg)
        return

    try:
        # Initialize components
        generator = ShopifyBulkMutationGenerator(csv_filepath, location_id)
        operator = ShopifyBulkOperator(shopify_domain, admin_api_token, api_version)

        # Generate files
        update_queue.put({"type": "status", "message": "Generating mutation files..."})
        generator.generate_mutation_queries()

        update_queue.put({"type": "status", "message": "Processing CSV data..."})
        success, stats = generator.process_csv()
        if not success:
            raise Exception("Failed to process CSV data")

        if dry_run:
            update_queue.put({
                "type": "complete",
                "status": "Dry run completed",
                "stats": stats,
                "time_taken": time.time() - start_time
            })
            return

        # Process each operation type
        operations = [
            ("variant", "variant_updates.jsonl", "variant_mutation.graphql"),
            ("product", "product_updates.jsonl", "product_mutation.graphql")
            #("inventory", "inventory_updates.jsonl", "inventory_mutation.graphql")
        ]

        results = {
            "total_operations": len(operations),
            "completed": 0,
            "failed": 0,
            "details": []
        }

        os.makedirs(output_results_dir, exist_ok=True)

        for op_type, jsonl_file, mutation_file in operations:
            op_result = {
                "type": op_type,
                "status": "pending",
                "success_count": 0,
                "failure_count": 0
            }

            try:
                update_queue.put({
                    "type": "status", 
                    "message": f"Starting {op_type} operation..."
                })

                # Load mutation query
                with open(mutation_file, "r") as f:
                    mutation_query = f.read().strip()

                # Create staged upload
                upload_url, params = operator._staged_upload_create(jsonl_file)
                staged_path = next(
                    (p['value'] for p in params if p['name'] == 'key'), 
                    None
                )
                if not staged_path:
                    raise Exception("Could not get staged upload path")

                # Upload file
                operator._upload_jsonl_file(jsonl_file, upload_url, params)

                # Run operation
                operation_id = operator._run_bulk_mutation(mutation_query, staged_path)
                final_status = operator._monitor_operation(operation_id, update_queue)

                if final_status['status'] != "COMPLETED":
                    raise Exception(f"Operation failed with status: {final_status['status']}")

                # Download results
                result_file = os.path.join(
                    output_results_dir, 
                    f"{op_type}_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.jsonl"
                )
                
                if operator.download_results(final_status['url'], result_file):
                    success, failure, _ = operator.process_result_file(result_file)
                    op_result.update({
                        "status": "completed",
                        "success_count": success,
                        "failure_count": failure,
                        "result_file": result_file
                    })
                    results["completed"] += 1
                else:
                    raise Exception("Failed to download results")

            except Exception as e:
                logger.error(f"{op_type} operation failed: {str(e)}")
                op_result.update({
                    "status": "failed",
                    "error": str(e)
                })
                results["failed"] += 1
                update_queue.put({
                    "type": "error",
                    "message": f"{op_type} operation failed: {str(e)}"
                })

            finally:
                results["details"].append(op_result)

        # Clean up temporary files
        for f in [f[1] for f in operations] + [f[2] for f in operations]:
            if os.path.exists(f):
                os.remove(f)

        update_queue.put({
            "type": "complete",
            "status": "Completed" if results["failed"] == 0 else "Completed with errors",
            "stats": stats,
            "results": results,
            "time_taken": time.time() - start_time
        })

    except Exception as e:
        logger.error(f"Bulk update process failed: {str(e)}")
        update_queue.put({
            "type": "error",
            "message": f"Process failed: {str(e)}"
        })