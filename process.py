import os
import json
import logging
import time
from datetime import datetime
from multiprocessing import Queue
from dotenv import load_dotenv
from backend import (
    ShopifyBulkMutationGenerator,
    ShopifyBulkOperator,
    ShopifyInventoryUpdater
)

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

def run_bulk_update_process(csv_filepath: str, output_results_dir: str, 
                           update_queue: Queue, dry_run: bool = False):
    """Main function to run the bulk update process with inventory support."""
    start_time = time.time()
    process_stats = {}
    inventory_results = {}

    try:
        # Load environment configuration
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

        # Initialize components
        update_queue.put({"type": "status", "message": "Initializing components..."})
        generator = ShopifyBulkMutationGenerator(csv_filepath, location_id)
        bulk_operator = ShopifyBulkOperator(shopify_domain, admin_api_token, api_version)
        inventory_updater = ShopifyInventoryUpdater(shopify_domain, admin_api_token, api_version)

        # Generate mutation files
        update_queue.put({"type": "status", "message": "Generating mutation files..."})
        generator.generate_mutation_queries()

        # Process CSV data
        update_queue.put({"type": "status", "message": "Processing CSV data..."})
        csv_success, process_stats = generator.process_csv()
        if not csv_success:
            raise Exception("Failed to process CSV data")

        update_queue.put({
            "type": "progress",
            "stats": process_stats
        })

        if dry_run:
            update_queue.put({
                "type": "complete",
                "status": "Dry run completed",
                "stats": process_stats,
                "time_taken": time.time() - start_time
            })
            return

        # Process bulk operations (products and variants)
        operations = [
            ("variant", "variant_updates.jsonl", "variant_mutation.graphql"),
            ("product", "product_updates.jsonl", "product_mutation.graphql")
        ]

        bulk_results = {
            "total_operations": len(operations),
            "completed": 0,
            "failed": 0,
            "details": []
        }

        os.makedirs(output_results_dir, exist_ok=True)

        # Process product and variant bulk operations
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
                    "message": f"Starting {op_type} bulk operation..."
                })

                # Load mutation query
                with open(mutation_file, "r") as f:
                    mutation_query = f.read().strip()

                # Create staged upload
                upload_url, params = bulk_operator._staged_upload_create(jsonl_file)
                staged_path = next(
                    (p['value'] for p in params if p['name'] == 'key'), 
                    None
                )
                if not staged_path:
                    raise Exception("Could not get staged upload path")

                # Upload file
                bulk_operator._upload_jsonl_file(jsonl_file, upload_url, params)

                # Run bulk operation
                operation_id = bulk_operator._run_bulk_mutation(mutation_query, staged_path)
                final_status = bulk_operator._monitor_operation(operation_id, update_queue)

                if final_status['status'] != "COMPLETED":
                    raise Exception(f"Operation failed with status: {final_status['status']}")

                # Download and process results
                result_file = os.path.join(
                    output_results_dir, 
                    f"{op_type}_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.jsonl"
                )
                
                if bulk_operator.download_results(final_status['url'], result_file):
                    success, failure, _ = bulk_operator.process_result_file(result_file)
                    op_result.update({
                        "status": "completed",
                        "success_count": success,
                        "failure_count": failure,
                        "result_file": result_file
                    })
                    bulk_results["completed"] += 1
                else:
                    raise Exception("Failed to download results")

            except Exception as e:
                logger.error(f"{op_type} bulk operation failed: {str(e)}")
                op_result.update({
                    "status": "failed",
                    "error": str(e)
                })
                bulk_results["failed"] += 1
                update_queue.put({
                    "type": "error",
                    "message": f"{op_type} bulk operation failed: {str(e)}"
                })

            finally:
                bulk_results["details"].append(op_result)

        # Process inventory updates
        inventory_stats = {}
        if generator.inventory_changes:
            try:
                update_queue.put({
                    "type": "status",
                    "message": f"Starting inventory updates ({len(generator.inventory_changes)} changes)..."
                })

                # Process inventory updates in batches
                inventory_results = inventory_updater.process_inventory_updates(
                    generator.inventory_changes,
                    update_queue
                )

                # Update statistics
                process_stats['inventory_updates'] = inventory_results.get('successful_updates', 0)
                process_stats['inventory_errors'] = inventory_results.get('failed_updates', 0)
                inventory_stats = {
                    'total_changes': inventory_results['total_changes'],
                    'successful': inventory_results['successful_updates'],
                    'failed': inventory_results['failed_updates']
                }

                # Save inventory results
                inv_result_file = os.path.join(
                    output_results_dir,
                    f"inventory_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
                )
                with open(inv_result_file, 'w') as f:
                    json.dump(inventory_results, f)

                update_queue.put({
                    "type": "status",
                    "message": f"Inventory updates completed: {inventory_stats['successful']} successful, {inventory_stats['failed']} failed"
                })

            except Exception as e:
                logger.error(f"Inventory updates failed: {str(e)}")
                update_queue.put({
                    "type": "error",
                    "message": f"Inventory updates failed: {str(e)}"
                })
                process_stats['inventory_errors'] = len(generator.inventory_changes)

        # Clean up temporary files
        cleanup_files = [
            "variant_mutation.graphql",
            "product_mutation.graphql",
            "variant_updates.jsonl",
            "product_updates.jsonl"
        ]
        
        for f in cleanup_files:
            if os.path.exists(f):
                os.remove(f)

        # Prepare final results
        final_results = {
            "status": "Completed" if bulk_results["failed"] == 0 else "Completed with errors",
            "time_taken": round(time.time() - start_time, 2),
            "bulk_operations": bulk_results,
            "inventory_operations": inventory_stats,
            "csv_stats": process_stats
        }

        update_queue.put({
            "type": "complete",
            "results": final_results
        })

    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        update_queue.put({
            "type": "error",
            "message": f"Process failed: {str(e)}"
        })
        return {
            "status": "Failed",
            "error": str(e),
            "time_taken": round(time.time() - start_time, 2)
        }

if __name__ == "__main__":
    # Example usage
    from multiprocessing import Queue
    queue = Queue()
    
    run_bulk_update_process(
        csv_filepath="sample.csv",
        output_results_dir="./results",
        update_queue=queue,
        dry_run=False
    )