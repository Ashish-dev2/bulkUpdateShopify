from multiprocessing import Queue
from process import run_bulk_update_process

queue = Queue()
run_bulk_update_process(
    csv_filepath=r"C:\Users\office\Desktop\Ashish\productDetailUpdate\bulkOperationRunMutation\test_CSV.csv",
    output_results_dir="./results",
    update_queue=queue,
    dry_run=False  
)