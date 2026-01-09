"""
Fraud Analysis Web Application.

A Streamlit-based web application for law enforcement cybercrime departments
to analyze and consolidate fraud transaction data from Excel files.
"""

import sys
import os

# Ensure the project root is on sys.path so `import src.xyz` works when
# Streamlit runs `src/app.py` as a script. This makes imports stable whether
# you run `python -m streamlit run src/app.py` from the repo root or from
# another working directory.
_THIS_FILE = os.path.abspath(__file__)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_THIS_FILE))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
from typing import Optional, List, Dict, Any

from src.upload_service import UploadService
from src.column_detector import ColumnDetector
from src.data_processor import DataProcessor
from src.validation_engine import ValidationEngine
from src.aggregation_engine import AggregationEngine
from src.report_generator import ReportGenerator
from src.dashboard import Dashboard
from src.session_manager import SessionManager
from src.models import ColumnMapping, AggregatedAccount, ProcessingStats, ValidationResult
from src.district_data import render_district_download_page
from src.merge_files import render_merge_files_page
from src.excel_merger import render_excel_merger_page

# Page configuration
st.set_page_config(
    page_title="Fraud Analysis Tool",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize services (cached to avoid recreation)
@st.cache_resource
def get_services():
    """Initialize and cache service instances."""
    return {
        'upload_service': UploadService(),
        'column_detector': ColumnDetector(),
        'data_processor': DataProcessor(),
        'validation_engine': ValidationEngine(),
        'aggregation_engine': AggregationEngine(),
        'report_generator': ReportGenerator(),
        'dashboard': Dashboard(),
        'session_manager': SessionManager()
    }


def init_session_state():
    """Initialize session state variables."""
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'upload'
    if 'uploaded_df' not in st.session_state:
        st.session_state.uploaded_df = None
    if 'filename' not in st.session_state:
        st.session_state.filename = None
    if 'column_mapping' not in st.session_state:
        st.session_state.column_mapping = None
    if 'cleaned_df' not in st.session_state:
        st.session_state.cleaned_df = None
    if 'validation_result' not in st.session_state:
        st.session_state.validation_result = None
    if 'aggregated_accounts' not in st.session_state:
        st.session_state.aggregated_accounts = None
    if 'processing_stats' not in st.session_state:
        st.session_state.processing_stats = None
    if 'processing_logs' not in st.session_state:
        st.session_state.processing_logs = []


def render_sidebar():
    """Render the navigation sidebar."""
    with st.sidebar:
        st.title("Navigation")
        
        # Page navigation
        pages = {
            'upload': '📤 Upload File',
            'mapping': '🔗 Column Mapping',
            'processing': '⚙️ Processing',
            'results': '📊 Results Dashboard',
            'district_download': '📍 District Data Download',
            'excel_merger': '📎 Merge Excel Files'
        }
        
        for page_key, page_name in pages.items():
            # Determine if page should be enabled
            enabled = True
            if page_key == 'mapping' and st.session_state.uploaded_df is None:
                enabled = False
            elif page_key == 'processing' and st.session_state.column_mapping is None:
                enabled = False
            elif page_key == 'results' and st.session_state.aggregated_accounts is None:
                enabled = False
            # district_download is always enabled
            
            if enabled:
                if st.button(page_name, key=f"nav_{page_key}", use_container_width=True):
                    st.session_state.current_page = page_key
                    st.rerun()
            else:
                st.button(page_name, key=f"nav_{page_key}", use_container_width=True, disabled=True)
        
        st.markdown("---")
        
        # Session info
        st.caption("Session Info")
        if st.session_state.filename:
            st.text(f"File: {st.session_state.filename}")
        
        st.markdown("---")
        
        # Data handling reminder
        st.caption("🔒 Security")
        st.caption("All data is processed in-memory only.")
        
        # Reset button
        st.markdown("---")
        if st.button("🔄 Start Over", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


def render_upload_page():
    """Render the file upload page with multiple file selection support."""
    services = get_services()
    upload_service = services['upload_service']
    
    st.title("📤 Upload Transaction Files")
    
    # Check if data already processed - show option to proceed or upload new
    if st.session_state.uploaded_df is not None:
        st.success(f"✅ Data already loaded: {len(st.session_state.uploaded_df)} rows from {st.session_state.filename}")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Proceed to Column Mapping", type="primary", use_container_width=True):
                st.session_state.current_page = 'mapping'
                st.rerun()
        with col2:
            if st.button("🔄 Upload New Files", use_container_width=True):
                st.session_state.uploaded_df = None
                st.session_state.filename = None
                st.session_state.column_mapping = None
                st.session_state.cleaned_df = None
                st.session_state.validation_result = None
                st.session_state.aggregated_accounts = None
                st.session_state.processing_stats = None
                st.rerun()
        
        # Show preview of current data
        st.markdown("---")
        st.subheader("📋 Current Data Preview")
        preview_df = upload_service.get_preview(st.session_state.uploaded_df, rows=10)
        st.dataframe(preview_df, use_container_width=True)
        return
    
    st.markdown("Upload **1 to 15 Excel/CSV files** using **Ctrl+Click** to select multiple files, then click **Process Files**.")
    
    # Multiple file uploader with Ctrl+Click support
    uploaded_files = st.file_uploader(
        "Choose Excel/CSV files (Ctrl+Click for multiple)",
        type=['xlsx', 'xls', 'csv'],
        accept_multiple_files=True,
        help="Supported formats: Excel (.xlsx, .xls) and CSV (.csv). Use Ctrl+Click to select multiple files."
    )
    
    # Show uploaded files list
    if uploaded_files:
        st.subheader(f"📁 {len(uploaded_files)} file(s) selected:")
        for f in uploaded_files:
            size_kb = f.size / 1024
            if size_kb > 1024:
                size_str = f"{size_kb/1024:.2f} MB"
            else:
                size_str = f"{size_kb:.2f} KB"
            st.write(f"• {f.name} — {size_str}")
        
        # Process Files button
        if st.button("🚀 Process Files", type="primary", use_container_width=True):
            if len(uploaded_files) > 15:
                st.warning("⚠️ Maximum 15 files allowed. Please remove some files.")
            else:
                all_data = []
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i, uploaded_file in enumerate(uploaded_files):
                    status_text.text(f"Reading file {i+1}/{len(uploaded_files)}: {uploaded_file.name}...")
                    progress_bar.progress((i + 1) / len(uploaded_files))
                    
                    try:
                        # Validate file
                        file_bytes = BytesIO(uploaded_file.getvalue())
                        validation_result = upload_service.validate_file(file_bytes, uploaded_file.name)
                        
                        if not validation_result.is_valid:
                            st.warning(f"⚠️ Skipping {uploaded_file.name}: {validation_result.error_message}")
                            continue
                        
                        # Read file
                        file_bytes.seek(0)
                        df = upload_service.read_file(file_bytes, uploaded_file.name)
                        
                        st.success(f"✅ {uploaded_file.name}: {len(df)} rows loaded")
                        all_data.append((uploaded_file.name, df))
                        
                    except Exception as e:
                        st.error(f"❌ Error reading {uploaded_file.name}: {str(e)}")
                
                progress_bar.progress(100)
                status_text.text("✅ All files processed!")
                
                if all_data:
                    # Combine all dataframes
                    combined_df = pd.concat([df for _, df in all_data], ignore_index=True)
                    
                    # Store in session state immediately
                    st.session_state.uploaded_df = combined_df
                    st.session_state.filename = f"{len(all_data)}_files_combined"
                    
                    # Rerun to show the proceed option
                    st.rerun()
                else:
                    st.error("❌ No valid data found in uploaded files.")
    else:
        st.info("📤 Select files using Ctrl+Click for multiple selection, then click **Process Files**.")


def render_mapping_page():
    """Render the column mapping page."""
    services = get_services()
    column_detector = services['column_detector']
    
    if st.session_state.uploaded_df is None:
        st.warning("Please upload a file first.")
        return
    
    df = st.session_state.uploaded_df
    headers = list(df.columns)
    
    st.title("🔗 Column Mapping")
    st.markdown("Map your file columns to the required fields. Auto-detected mappings are shown below.")
    
    # Auto-detect columns
    auto_mapping = column_detector.detect_columns(headers)
    
    # Show confidence scores
    if auto_mapping.confidence_scores:
        st.subheader("🎯 Auto-Detection Confidence")
        conf_cols = st.columns(4)
        for i, (col_type, score) in enumerate(auto_mapping.confidence_scores.items()):
            with conf_cols[i % 4]:
                color = "green" if score >= 0.9 else "orange" if score >= 0.8 else "red"
                st.markdown(f"**{col_type.replace('_', ' ').title()}**: :{color}[{score:.0%}]")
    
    # Show ambiguous mappings warning
    if auto_mapping.ambiguous_mappings:
        st.warning("⚠️ Some columns have ambiguous mappings. Please verify the selections below.")
    
    st.markdown("---")
    st.subheader("📝 Column Assignments")
    
    # Create mapping form
    col1, col2 = st.columns(2)
    
    # Add "None" option to headers
    header_options = ["-- Not Mapped --"] + headers
    
    def get_default_index(mapped_value):
        if mapped_value and mapped_value in headers:
            return headers.index(mapped_value) + 1
        return 0
    
    with col1:
        bank_account_col = st.selectbox(
            "Bank Account Number",
            options=header_options,
            index=get_default_index(auto_mapping.bank_account_number),
            help="Column containing fraudster bank account numbers"
        )
        
        amount_col = st.selectbox(
            "Amount",
            options=header_options,
            index=get_default_index(auto_mapping.amount),
            help="Column containing transaction amounts"
        )
        
        ack_col = st.selectbox(
            "Acknowledgement Number",
            options=header_options,
            index=get_default_index(auto_mapping.acknowledgement_number),
            help="Column containing acknowledgement/reference numbers"
        )
        
        ifsc_col = st.selectbox(
            "IFSC Code",
            options=header_options,
            index=get_default_index(auto_mapping.ifsc_code),
            help="Column containing bank IFSC codes"
        )
    
    with col2:
        bank_name_col = st.selectbox(
            "Bank Name",
            options=header_options,
            index=get_default_index(auto_mapping.bank_name),
            help="Column containing bank names"
        )
        
        address_col = st.selectbox(
            "Address",
            options=header_options,
            index=get_default_index(auto_mapping.address),
            help="Column containing beneficiary addresses"
        )
        
        disputed_col = st.selectbox(
            "Disputed Amount",
            options=header_options,
            index=get_default_index(auto_mapping.disputed_amount),
            help="Column containing disputed/chargeback amounts"
        )
        
        serial_col = st.selectbox(
            "Serial Number",
            options=header_options,
            index=get_default_index(auto_mapping.serial_number),
            help="Column containing serial/row numbers"
        )
        
        district_col = st.selectbox(
            "District",
            options=header_options,
            index=get_default_index(auto_mapping.district),
            help="Column containing district names"
        )
        
        state_col = st.selectbox(
            "State",
            options=header_options,
            index=get_default_index(auto_mapping.state),
            help="Column containing state names"
        )


    # Validation
    st.markdown("---")
    
    st.success("✅ Map the columns that are available in your file")
    
    # Proceed button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Proceed to Processing", type="primary", use_container_width=True):
            # Create final mapping
            final_mapping = ColumnMapping(
                serial_number=serial_col if serial_col != "-- Not Mapped --" else None,
                acknowledgement_number=ack_col if ack_col != "-- Not Mapped --" else None,
                bank_account_number=bank_account_col if bank_account_col != "-- Not Mapped --" else None,
                ifsc_code=ifsc_col if ifsc_col != "-- Not Mapped --" else None,
                address=address_col if address_col != "-- Not Mapped --" else None,
                amount=amount_col if amount_col != "-- Not Mapped --" else None,
                disputed_amount=disputed_col if disputed_col != "-- Not Mapped --" else None,
                bank_name=bank_name_col if bank_name_col != "-- Not Mapped --" else None,
                district=district_col if district_col != "-- Not Mapped --" else None,
                state=state_col if state_col != "-- Not Mapped --" else None
            )
            
            st.session_state.column_mapping = final_mapping
            st.session_state.current_page = 'processing'
            st.rerun()


def render_processing_page():
    """Render the data processing page with progress tracking."""
    services = get_services()
    data_processor = services['data_processor']
    validation_engine = services['validation_engine']
    aggregation_engine = services['aggregation_engine']
    dashboard = services['dashboard']
    
    if st.session_state.uploaded_df is None or st.session_state.column_mapping is None:
        st.warning("Please complete the previous steps first.")
        return
    
    st.title("⚙️ Data Processing")
    
    df = st.session_state.uploaded_df
    mapping = st.session_state.column_mapping
    
    # Check if already processed
    if st.session_state.aggregated_accounts is not None:
        st.success("✅ Data has already been processed!")
        st.info("Click 'View Results' to see the dashboard, or 'Reprocess' to process again.")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("View Results", type="primary", use_container_width=True):
                st.session_state.current_page = 'results'
                st.rerun()
        with col2:
            if st.button("Reprocess Data", use_container_width=True):
                st.session_state.aggregated_accounts = None
                st.session_state.processing_stats = None
                st.session_state.cleaned_df = None
                st.session_state.validation_result = None
                st.session_state.processing_logs = []
                st.rerun()
        return
    
    # Processing controls
    if st.button("🚀 Start Processing", type="primary", use_container_width=True):
        logs = []
        
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.container()
        
        # Metrics placeholders
        metrics_cols = st.columns(4)
        rows_metric = metrics_cols[0].empty()
        accounts_metric = metrics_cols[1].empty()
        errors_metric = metrics_cols[2].empty()
        amount_metric = metrics_cols[3].empty()
        
        total_rows = len(df)
        
        try:
            # Step 1: Data Cleaning (20%)
            status_text.text("Step 1/4: Cleaning data...")
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Starting data cleaning...")
            
            cleaned_df = data_processor.clean_dataframe(df, mapping)
            rows_after_cleaning = len(cleaned_df)
            
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Removed {total_rows - rows_after_cleaning} empty rows")
            progress_bar.progress(20)
            rows_metric.metric("Rows Processed", rows_after_cleaning)
            
            # Step 2: Validation (40%)
            status_text.text("Step 2/4: Validating data...")
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Starting data validation...")
            
            validation_result = validation_engine.validate_dataframe(cleaned_df, mapping)
            
            if not validation_result.is_valid:
                for error in validation_result.critical_errors:
                    logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] CRITICAL: {error}")
                st.error("❌ Critical validation errors found. Cannot proceed.")
                for error in validation_result.critical_errors:
                    st.error(error)
                return
            
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(validation_result.warnings)} warnings")
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Flagged {len(validation_result.flagged_rows)} rows")
            progress_bar.progress(40)
            errors_metric.metric("Errors Found", len(validation_result.flagged_rows))


            # Step 3: Aggregation (70%)
            status_text.text("Step 3/4: Aggregating by account...")
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Starting aggregation by account number...")
            
            aggregated = aggregation_engine.aggregate_by_account(cleaned_df, mapping)
            sorted_accounts = aggregation_engine.sort_results(aggregated)
            
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(sorted_accounts)} unique accounts")
            progress_bar.progress(70)
            accounts_metric.metric("Unique Accounts", len(sorted_accounts))
            
            # Step 4: Calculate Statistics (100%)
            status_text.text("Step 4/4: Calculating statistics...")
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Calculating summary statistics...")
            
            stats = dashboard.calculate_statistics(
                accounts=sorted_accounts,
                total_input_rows=total_rows,
                input_filename=st.session_state.filename or "",
                rows_with_errors=len(validation_result.flagged_rows)
            )
            
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Total fraud amount: ₹{stats.total_fraud_amount:,.2f}")
            progress_bar.progress(100)
            amount_metric.metric("Total Amount", f"₹{stats.total_fraud_amount:,.0f}")
            
            # Store results
            st.session_state.cleaned_df = cleaned_df
            st.session_state.validation_result = validation_result
            st.session_state.aggregated_accounts = sorted_accounts
            st.session_state.processing_stats = stats
            st.session_state.processing_logs = logs
            
            status_text.text("✅ Processing complete!")
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] Processing completed successfully!")
            
            st.success("🎉 Processing completed successfully!")
            
            # Show logs
            with log_container:
                st.subheader("📜 Processing Log")
                log_text = "\n".join(logs)
                st.text_area("Logs", value=log_text, height=200, disabled=True)
            
            # Proceed button
            if st.button("View Results Dashboard", type="primary"):
                st.session_state.current_page = 'results'
                st.rerun()
                
        except Exception as e:
            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: {str(e)}")
            st.error(f"❌ Processing failed: {str(e)}")
            
            with log_container:
                st.subheader("📜 Processing Log")
                log_text = "\n".join(logs)
                st.text_area("Logs", value=log_text, height=200, disabled=True)


def render_results_page():
    """Render the results dashboard with statistics, downloads, and filters."""
    services = get_services()
    dashboard = services['dashboard']
    report_generator = services['report_generator']
    
    if st.session_state.aggregated_accounts is None:
        st.warning("Please process data first.")
        return
    
    accounts = st.session_state.aggregated_accounts
    stats = st.session_state.processing_stats
    validation_result = st.session_state.validation_result
    
    st.title("📊 Results Dashboard")
    
    # Summary Statistics
    st.subheader("📈 Summary Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Input Rows", stats.total_input_rows)
    with col2:
        st.metric("Unique Accounts", stats.unique_accounts)
    with col3:
        st.metric("Total Fraud Amount", f"₹{stats.total_fraud_amount:,.2f}")
    with col4:
        st.metric("Avg Amount/Account", f"₹{stats.average_amount_per_account:,.2f}")
    
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric("Rows Processed", stats.rows_processed)
    with col6:
        st.metric("Rows with Errors", stats.rows_with_errors)
    with col7:
        st.metric("Total Disputed", f"₹{stats.total_disputed_amount:,.2f}")
    with col8:
        if stats.unique_accounts > 0:
            avg_txn = stats.total_input_rows / stats.unique_accounts
            st.metric("Avg Txn/Account", f"{avg_txn:.1f}")
        else:
            st.metric("Avg Txn/Account", "0")
    
    st.markdown("---")
    
    # Download Section
    st.subheader("📥 Download Reports")
    
    download_cols = st.columns(4)
    
    with download_cols[0]:
        excel_bytes = report_generator.generate_excel_bytes(accounts)
        st.download_button(
            label="📊 Download Excel",
            data=excel_bytes,
            file_name=f"fraud_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    with download_cols[1]:
        csv_bytes = report_generator.generate_csv_bytes(accounts)
        st.download_button(
            label="📄 Download CSV",
            data=csv_bytes,
            file_name=f"fraud_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with download_cols[2]:
        quality_metrics = validation_result.quality_report if validation_result else None
        pdf_bytes = report_generator.generate_pdf_bytes(accounts, stats, quality_metrics)
        st.download_button(
            label="📑 Download PDF",
            data=pdf_bytes,
            file_name=f"fraud_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            mime="application/pdf",
            use_container_width=True
        )
    
    with download_cols[3]:
        errors = validation_result.warnings if validation_result else []
        audit_log = report_generator.generate_audit_log(
            input_filename=st.session_state.filename or "",
            rows_processed=stats.rows_processed,
            errors_encountered=errors,
            timestamp=stats.processing_timestamp
        )
        st.download_button(
            label="📋 Download Audit Log",
            data=audit_log.encode('utf-8'),
            file_name=f"audit_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            mime="text/plain",
            use_container_width=True
        )


    st.markdown("---")
    
    # Search and Filter Section
    st.subheader("🔍 Search & Filter")
    
    filter_cols = st.columns(3)
    
    with filter_cols[0]:
        search_query = st.text_input(
            "Search Account Number",
            placeholder="Enter account number to search...",
            help="Search for specific account numbers"
        )
    
    with filter_cols[1]:
        min_transactions = st.number_input(
            "Minimum Transactions",
            min_value=0,
            value=0,
            help="Filter accounts with at least this many transactions"
        )
    
    with filter_cols[2]:
        min_amount = st.number_input(
            "Minimum Amount (₹)",
            min_value=0.0,
            value=0.0,
            help="Filter accounts with at least this total amount"
        )
    
    # Apply filters
    filtered_accounts = accounts
    
    if search_query:
        filtered_accounts = dashboard.search_accounts(filtered_accounts, search_query)
    
    if min_transactions > 0:
        filtered_accounts = dashboard.filter_by_min_transactions(filtered_accounts, min_transactions)
    
    if min_amount > 0:
        filtered_accounts = dashboard.filter_by_min_amount(filtered_accounts, min_amount)
    
    # Show filter results count
    st.info(f"Showing {len(filtered_accounts)} of {len(accounts)} accounts")
    
    st.markdown("---")
    
    # Top 10 Accounts
    st.subheader("🏆 Top 10 Accounts by Amount")
    
    top_10 = filtered_accounts[:10] if len(filtered_accounts) >= 10 else filtered_accounts
    
    if top_10:
        top_10_data = []
        for i, acc in enumerate(top_10, 1):
            top_10_data.append({
                "Rank": i,
                "Account Number": acc.account_number,
                "Bank Name": acc.bank_name,
                "IFSC Code": acc.ifsc_code,
                "Transactions": acc.total_transactions,
                "Total Amount": f"₹{acc.total_amount:,.2f}",
                "Risk Score": f"{acc.risk_score:.1f}"
            })
        
        st.dataframe(
            pd.DataFrame(top_10_data),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No accounts match the current filters.")


    st.markdown("---")
    
    # Full Results Table
    st.subheader("📋 All Results")
    
    # Pagination
    items_per_page = st.selectbox("Items per page", [10, 25, 50, 100], index=1)
    total_pages = max(1, (len(filtered_accounts) + items_per_page - 1) // items_per_page)
    
    page_col1, page_col2, page_col3 = st.columns([1, 2, 1])
    with page_col2:
        current_page = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=1,
            help=f"Total pages: {total_pages}"
        )
    
    # Get page data
    start_idx = (current_page - 1) * items_per_page
    end_idx = min(start_idx + items_per_page, len(filtered_accounts))
    page_accounts = filtered_accounts[start_idx:end_idx]
    
    if page_accounts:
        results_data = []
        for acc in page_accounts:
            results_data.append({
                "Account Number": acc.account_number,
                "Bank Name": acc.bank_name,
                "IFSC Code": acc.ifsc_code,
                "Address": acc.address[:50] + "..." if len(acc.address) > 50 else acc.address,
                "District": acc.district,
                "State": acc.state,
                "Transactions": acc.total_transactions,
                "Ack Numbers": acc.acknowledgement_numbers[:30] + "..." if len(acc.acknowledgement_numbers) > 30 else acc.acknowledgement_numbers,
                "Total Amount": f"₹{acc.total_amount:,.2f}",
                "Disputed Amount": f"₹{acc.total_disputed_amount:,.2f}",
                "Risk Score": f"{acc.risk_score:.1f}"
            })
        
        st.dataframe(
            pd.DataFrame(results_data),
            use_container_width=True,
            hide_index=True
        )
        
        st.caption(f"Showing {start_idx + 1}-{end_idx} of {len(filtered_accounts)} accounts")
    else:
        st.info("No accounts to display.")
    
    st.markdown("---")
    
    # Flagged/Error Rows Section
    if validation_result and validation_result.flagged_rows:
        with st.expander("⚠️ View Flagged Rows", expanded=False):
            st.warning(f"Found {len(validation_result.flagged_rows)} rows with issues")
            
            # Show warnings
            if validation_result.warnings:
                st.subheader("Warnings")
                for warning in validation_result.warnings[:50]:  # Limit to first 50
                    st.text(f"• {warning}")
                
                if len(validation_result.warnings) > 50:
                    st.text(f"... and {len(validation_result.warnings) - 50} more warnings")
    
    # Data Quality Report
    if validation_result and validation_result.quality_report:
        with st.expander("📊 Data Quality Report", expanded=False):
            qr = validation_result.quality_report
            
            quality_cols = st.columns(4)
            with quality_cols[0]:
                st.metric("Total Rows", qr.get('total_rows', 0))
            with quality_cols[1]:
                st.metric("Valid Accounts", qr.get('valid_account_numbers', 0))
            with quality_cols[2]:
                st.metric("Valid IFSC Codes", qr.get('valid_ifsc_codes', 0))
            with quality_cols[3]:
                st.metric("Valid Amounts", qr.get('valid_amounts', 0))
            
            quality_cols2 = st.columns(4)
            with quality_cols2[0]:
                st.metric("Account Validity Rate", f"{qr.get('account_number_validity_rate', 0):.1f}%")
            with quality_cols2[1]:
                st.metric("IFSC Validity Rate", f"{qr.get('ifsc_validity_rate', 0):.1f}%")
            with quality_cols2[2]:
                st.metric("Amount Validity Rate", f"{qr.get('amount_validity_rate', 0):.1f}%")
            with quality_cols2[3]:
                st.metric("Data Completeness", f"{qr.get('data_completeness_rate', 0):.1f}%")


def main():
    """Main application entry point."""
    # Initialize session state
    init_session_state()
    
    # Render sidebar navigation
    render_sidebar()
    
    # Render current page
    page = st.session_state.current_page
    
    if page == 'upload':
        render_upload_page()
    elif page == 'mapping':
        render_mapping_page()
    elif page == 'processing':
        render_processing_page()
    elif page == 'results':
        render_results_page()
    elif page == 'district_download':
        render_district_download_page()
    elif page == 'excel_merger':
        render_merge_files_page()
    else:
        render_upload_page()


if __name__ == "__main__":
    main()
