import streamlit as st
import pandas as pd
from datetime import datetime
import os
import time
from multiprocessing import Process, Queue
from dotenv import load_dotenv

# Load environment variables once for the main Streamlit process
load_dotenv()

# Import the background worker function from updateProcess.py
# Ensure updateProcess.py is in the same directory
from process import run_bulk_update_process

# Configure Streamlit page
st.set_page_config(page_title="Shopify Bulk Updater", layout="wide")

# Main title
st.title("Shopify Product & Variant Bulk Updater")

# Initialize session state variables for process management and results
if 'process' not in st.session_state:
    st.session_state.process = None
if 'update_queue' not in st.session_state:
    st.session_state.update_queue = None
if 'current_status' not in st.session_state:
    st.session_state.current_status = "Waiting for CSV upload..."
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'progress_text' not in st.session_state:
    st.session_state.progress_text = "Idle"
if 'final_results' not in st.session_state:
    st.session_state.final_results = None
if 'progress_value' not in st.session_state:
    st.session_state.progress_value = 0 # Initialize progress value

# Sidebar for settings and instructions
st.sidebar.title("⚙️ Settings & Instructions")
dry_run = st.sidebar.checkbox(
    "Dry Run Mode (No actual Shopify changes)",
    value=True,
    help="When checked, the process will generate JSONL files but will NOT make actual calls to Shopify's Bulk API. Useful for testing CSV parsing and JSONL generation locally."
)
show_raw_data = st.sidebar.checkbox("Show Raw Data Preview", value=True)

st.sidebar.markdown("""
    ### 📋 How to Use:
    1.  **Upload CSV:** Upload your product/variant data in CSV format.
    2.  **Review Preview:** (Optional) Check the CSV data preview.
    3.  **Configure Settings:** Use the 'Dry Run Mode' checkbox.
    4.  **Start Processing:** Click the '🚀 Start Processing' button.
    5.  **Monitor Progress:** Watch the live log and status updates.
    6.  **Download Results:** Once complete, download the result files.

    ### ⚠️ Important:
    -   Ensure your `.env` file (in the same directory) has `SHOPIFY_DOMAIN`, `DEMOATLANTIC_ADMIN_TOKEN`, `SHOPIFY_API_VERSION`, and `SHOPIFY_LOCATION_ID` configured correctly.
    -   This app uses `multiprocessing` for background tasks.
    -   **Inventory Updates:** Inventory quantities are handled as part of the variant update within `productVariantsBulkUpdate`.
    
    ### 📝 Required CSV Columns:
    -   `Command` (e.g., UPDATE)
    -   `ID` (Product ID)
    -   `Variant ID`
    -   `Price / India`
    -   `Compare At Price / India`
    -   `Variant Metafield: custom.original_price [number_decimal]`
    -   `Variant Metafield: custom.original_currency [single_line_text_field]`
    -   `Inventory Available: Head Office - DG`
    -   `Metafield: custom.sla [single_line_text_field]`
    -   `Metafield: custom.vendor_source [single_line_text_field]`
""")


# File upload section
st.header("1. Upload Your Product Data CSV")
uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"], 
                               help="Upload your product data for bulk updates.")

# Display file preview
if show_raw_data and uploaded_file:
    st.subheader("📄 CSV File Preview (First 5 rows)")
    try:
        uploaded_file.seek(0) # Reset file pointer to the beginning
        preview_df = pd.read_csv(uploaded_file)
        st.dataframe(preview_df.head())
    except Exception as e:
        st.error(f"Could not read file for preview: {str(e)}")

st.header("2. Start Bulk Update")
# UI Elements for Live Updates
progress_bar = st.progress(st.session_state.progress_value, text=st.session_state.progress_text)
status_message_placeholder = st.empty()
live_log_expander = st.expander("🔍 Live Update Log", expanded=False)
results_display_placeholder = st.empty()

# Function to update UI from queue
def update_ui_from_queue():
    while not st.session_state.update_queue.empty():
        message = st.session_state.update_queue.get()
        if message["type"] == "log":
            st.session_state.logs.append(message["message"])
        elif message["type"] == "status":
            st.session_state.current_status = message["message"]
        elif message["type"] == "progress":
            # Update progress text, value is handled by the overall progress bar
            st.session_state.progress_text = message["text"]
            # For a more granular progress bar, you'd need to calculate overall progress
            # based on total expected operations/items. For now, text updates are sufficient.
        elif message["type"] == "complete":
            st.session_state.final_results = message
            st.session_state.current_status = message["status"]
            st.session_state.progress_value = 100 # Set to 100% on completion
        elif message["type"] == "error":
            st.session_state.logs.append(f"ERROR: {message['message']}")
            st.session_state.current_status = f"Failed: {message['message']}"
            st.session_state.progress_value = 0 # Reset or set to error state
        
    # Always update the visible UI elements
    progress_bar.progress(st.session_state.progress_value, text=st.session_state.progress_text)
    status_message_placeholder.info(f"Current Status: {st.session_state.current_status}")
    
    with live_log_expander:
        for log_entry in st.session_state.logs[-50:]: # Display last 50 log entries
            st.text(log_entry)
        # Scroll to bottom of log
        st.markdown('<div id="bottom-of-log"></div>', unsafe_allow_html=True)
        st.components.v1.html("""
            <script>
                var element = document.getElementById("bottom-of-log");
                if (element) { element.scrollIntoView(); }
            </script>
        """, height=0)


# Main Processing Button
if st.button("🚀 Start Processing", type="primary", disabled=(st.session_state.process and st.session_state.process.is_alive())):
    if uploaded_file is None:
        st.error("Please upload a CSV file first.")
    else:
        # Reset session state for a new run
        st.session_state.current_status = "Initializing..."
        st.session_state.logs = []
        st.session_state.progress_value = 0
        st.session_state.progress_text = "Starting..."
        st.session_state.final_results = None

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        run_results_dir = os.path.join("bulk_update_results", f"run_{timestamp}")
        os.makedirs(run_results_dir, exist_ok=True)

        csv_file_path = os.path.join("temp_uploads", f"uploaded_csv_{timestamp}.csv")
        os.makedirs("temp_uploads", exist_ok=True)
        uploaded_file.seek(0)
        with open(csv_file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.session_state.logs.append(f"CSV file saved to: {csv_file_path}")
        
        st.session_state.update_queue = Queue()

        st.session_state.process = Process(
            target=run_bulk_update_process, 
            args=(csv_file_path, run_results_dir, st.session_state.update_queue, dry_run)
        )
        st.session_state.process.start()
        st.session_state.current_status = "Process started. Monitoring progress..."
        st.rerun() # Trigger rerun to start monitoring

# --- UI Polling Loop ---
# This loop continuously checks the queue and updates the UI
if st.session_state.process and st.session_state.process.is_alive():
    update_ui_from_queue()
    time.sleep(1) # Poll every 1 second
    st.rerun() # Rerun the app to update UI

# --- Display Final Results ---
if st.session_state.final_results:
    with results_display_placeholder.container():
        st.subheader("📊 Bulk Operation Summary")
        
        results = st.session_state.final_results
        
        st.markdown(f"**Overall Status:** `{results['status']}`")
        st.markdown(f"**Total CSV Rows Processed:** `{results['stats'].get('total_rows', 'N/A')}`")
        st.markdown(f"**CSV Rows Skipped (due to missing IDs/data):** `{results['stats'].get('skipped_rows', 'N/A')}`")
        st.markdown(f"**Time Taken:** `{results['time_taken']:.2f} seconds`")

        if 'dry_run_info' in results: # Check if dry run info is present
            st.info(results['dry_run_info'])
        else:
            st.subheader("Detailed Operation Results:")
            if results.get('operation_results') and results['operation_results'].get('details'):
                for op_detail in results['operation_results']['details']:
                    st.markdown(f"---")
                    st.markdown(f"**Operation Type:** `{op_detail['type'].upper()}`")
                    st.markdown(f"**Status:** `{op_detail['status']}`")
                    st.markdown(f"**Successful Updates:** `{op_detail.get('success_count', 0)}`")
                    st.markdown(f"**Failed Updates:** `{op_detail.get('failure_count', 0)}`")
                    
                    if op_detail.get('error'):
                        st.error(f"Error Message: {op_detail['error']}")
                    
                    if op_detail.get('result_file') and os.path.exists(op_detail['result_file']):
                        with open(op_detail['result_file'], "rb") as f:
                            st.download_button(
                                label=f"Download {op_detail['type'].upper()} Results",
                                data=f,
                                file_name=os.path.basename(op_detail['result_file']),
                                mime="application/jsonl"
                            )
                    if op_detail.get('errors'): # Detailed errors from Shopify's result file
                        with st.expander(f"View Detailed Errors for {op_detail['type'].upper()}"):
                            for err_entry in op_detail['errors']:
                                st.json(err_entry)
            else:
                st.info("No detailed operation results available.")

# Clean up temporary CSV file after process completes and results are displayed/downloaded
if st.session_state.process and not st.session_state.process.is_alive() and st.session_state.final_results:
    # Assuming csv_file_path is stored in session state or can be derived
    # For now, let's assume it's cleaned up by the worker process as per updateProcess.py's finally block
    pass

# Add some styling (optional)
st.markdown("""
<style>
    .stProgress > div > div > div > div {
        background-color: #4CAF50; /* Green progress bar */
    }
    .st-b7 { /* Adjust button text color if needed */
        color: white;
    }
</style>
""", unsafe_allow_html=True)

