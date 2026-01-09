"""
Merge Excel Files Module - Upload multiple files and get aggregated summary.

Features:
- Upload 1 to 15 Excel files
- Auto-detect columns (Account No, ACK No, Amount, etc.)
- Merge all files and aggregate by account
- Download summary as Excel/CSV
- Optimized for large files
"""
import streamlit as st
import pandas as pd
import io
from typing import List, Dict, Optional, Tuple


def auto_detect_columns(df: pd.DataFrame) -> Dict[str, str]:
    """Auto-detect column mappings based on column names."""
    # Normalize column names
    df.columns = [str(col).strip().lower() if col is not None else "" for col in df.columns]
    
    columns_map = {}
    
    for col in df.columns:
        col_lower = col.lower()
        
        # Acknowledgement Number
        if ('acknowledgement' in col_lower or 'ack' in col_lower) and ('no' in col_lower or 'number' in col_lower):
            columns_map['ack_no'] = col
        elif col_lower in ['acknowledgement no.', 'ack no.', 'ack_no', 'acknowledgement_no']:
            columns_map['ack_no'] = col
        
        # Account Number
        if 'account' in col_lower and ('no' in col_lower or 'number' in col_lower):
            columns_map['account_no'] = col
        elif col_lower in ['account no.', 'account_no', 'acc no.', 'acc_no']:
            columns_map['account_no'] = col
        
        # Transaction Amount
        if 'transaction' in col_lower and 'amount' in col_lower:
            columns_map['transaction_amount'] = col
        elif col_lower in ['transaction amount', 'txn amount', 'amount']:
            columns_map['transaction_amount'] = col
        
        # Disputed Amount
        if 'disputed' in col_lower and 'amount' in col_lower:
            columns_map['disputed_amount'] = col
        elif col_lower in ['disputed amount', 'dispute amount']:
            columns_map['disputed_amount'] = col
        
        # Bank Name
        if 'bank' in col_lower and 'name' not in columns_map.get('account_no', ''):
            if 'fi' in col_lower or 'name' in col_lower or col_lower == 'bank':
                columns_map['bank_name'] = col
        elif 'bank/fi' in col_lower or 'bank name' in col_lower:
            columns_map['bank_name'] = col
        
        # IFSC Code
        if 'ifsc' in col_lower:
            columns_map['ifsc_code'] = col
        
        # District
        if 'district' in col_lower:
            columns_map['district'] = col
        
        # State
        if 'state' in col_lower and 'district' not in col_lower:
            columns_map['state'] = col
        
        # Address
        if 'address' in col_lower:
            columns_map['address'] = col
    
    return columns_map


def read_excel_optimized(uploaded_file) -> pd.DataFrame:
    """Read Excel file with optimization for large files."""
    filename = uploaded_file.name.lower()
    
    if filename.endswith('.csv'):
        return pd.read_csv(uploaded_file, low_memory=False, dtype=str)
    else:
        return pd.read_excel(uploaded_file, dtype=str)


def process_single_file(uploaded_file, file_index: int) -> Tuple[Optional[pd.DataFrame], str]:
    """Process a single uploaded file and return standardized data."""
    try:
        df = read_excel_optimized(uploaded_file)
        original_cols = list(df.columns)
        
        # Auto-detect columns
        columns_map = auto_detect_columns(df.copy())
        
        # Check required columns
        required = {'account_no', 'transaction_amount'}
        missing = required - set(columns_map.keys())
        
        if missing:
            return None, f"Missing required columns: {missing}. Detected: {list(columns_map.keys())}"
        
        # Build standardized dataframe
        data = pd.DataFrame()
        
        # Required columns
        data['Account No.'] = df[columns_map['account_no']].astype(str).str.strip()
        data['Transaction Amount'] = pd.to_numeric(
            df[columns_map['transaction_amount']].astype(str).str.replace(',', '').str.strip(),
            errors='coerce'
        ).fillna(0)
        
        # Optional columns
        if 'ack_no' in columns_map:
            data['Acknowledgement No.'] = df[columns_map['ack_no']].astype(str).str.strip()
        else:
            data['Acknowledgement No.'] = ''
        
        if 'disputed_amount' in columns_map:
            data['Disputed Amount'] = pd.to_numeric(
                df[columns_map['disputed_amount']].astype(str).str.replace(',', '').str.strip(),
                errors='coerce'
            ).fillna(0)
        else:
            data['Disputed Amount'] = 0
        
        if 'bank_name' in columns_map:
            data['Bank Name'] = df[columns_map['bank_name']].astype(str).str.strip()
        else:
            data['Bank Name'] = 'Unknown'
        
        if 'ifsc_code' in columns_map:
            data['IFSC Code'] = df[columns_map['ifsc_code']].astype(str).str.strip()
        else:
            data['IFSC Code'] = ''
        
        if 'district' in columns_map:
            data['District'] = df[columns_map['district']].astype(str).str.strip()
        else:
            data['District'] = ''
        
        if 'state' in columns_map:
            data['State'] = df[columns_map['state']].astype(str).str.strip()
        else:
            data['State'] = ''
        
        if 'address' in columns_map:
            data['Address'] = df[columns_map['address']].astype(str).str.strip()
        else:
            data['Address'] = ''
        
        # Remove rows with no transaction amount
        data = data[data['Transaction Amount'] != 0]
        
        # Remove rows with empty account numbers
        data = data[data['Account No.'].str.strip() != '']
        data = data[data['Account No.'] != 'nan']
        
        return data, f"✅ {len(data):,} valid rows"
        
    except Exception as e:
        return None, f"❌ Error: {str(e)}"


def aggregate_data(combined_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate data by account number - OPTIMIZED."""
    
    # Group by Account No., Bank Name, IFSC Code
    grouped = combined_df.groupby(['Account No.', 'Bank Name', 'IFSC Code'], dropna=False)
    
    # Aggregations
    summary = grouped.agg({
        'Acknowledgement No.': lambda x: ';'.join(x.dropna().unique()),
        'Transaction Amount': 'sum',
        'Disputed Amount': 'sum',
        'District': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else '',
        'State': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else '',
        'Address': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else ''
    }).reset_index()
    
    # Add transaction count
    txn_counts = grouped.size().reset_index(name='Transaction Count')
    summary = summary.merge(txn_counts, on=['Account No.', 'Bank Name', 'IFSC Code'])
    
    # Count distinct ACK numbers
    summary['Distinct ACK Count'] = summary['Acknowledgement No.'].apply(
        lambda x: len(set(x.split(';'))) if x else 0
    )
    
    # Reorder columns
    summary = summary[[
        'Account No.', 'Bank Name', 'IFSC Code', 'District', 'State', 'Address',
        'Transaction Count', 'Distinct ACK Count', 'Acknowledgement No.',
        'Transaction Amount', 'Disputed Amount'
    ]]
    
    # Sort by Transaction Amount descending
    summary = summary.sort_values('Transaction Amount', ascending=False).reset_index(drop=True)
    
    return summary


def render_merge_files_page():
    """Render the Merge Excel Files page."""
    st.title("📂 Merge Excel Files")
    st.markdown("""
    Upload **1 to 15 Excel files**, auto-detect columns, merge and aggregate by account number.
    Download the summary directly as Excel or CSV.
    """)
    
    st.markdown("---")
    
    # File uploader
    uploaded_files = st.file_uploader(
        "Choose Excel/CSV files (1-15 files)",
        type=['xlsx', 'xls', 'csv'],
        accept_multiple_files=True,
        key="merge_file_uploader"
    )
    
    if uploaded_files:
        st.info(f"📁 **{len(uploaded_files)}** file(s) uploaded")
        
        # Show file list
        with st.expander("View uploaded files", expanded=False):
            for f in uploaded_files:
                size_kb = f.size / 1024
                if size_kb > 1024:
                    size_str = f"{size_kb/1024:.2f} MB"
                else:
                    size_str = f"{size_kb:.2f} KB"
                st.write(f"• **{f.name}** — {size_str}")
    
    st.markdown("---")
    
    # Process button
    process_btn = st.button("🚀 Process & Merge Files", type="primary", use_container_width=True)
    
    if process_btn:
        if not uploaded_files:
            st.warning("⚠️ Please upload at least 1 file")
            return
        
        if len(uploaded_files) > 15:
            st.warning("⚠️ Maximum 15 files allowed. Please remove some files.")
            return
        
        # Process each file
        all_data = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, uploaded_file in enumerate(uploaded_files):
            status_text.text(f"Processing {uploaded_file.name}...")
            progress_bar.progress((i + 1) / len(uploaded_files))
            
            data, message = process_single_file(uploaded_file, i)
            
            if data is not None:
                st.success(f"**{uploaded_file.name}**: {message}")
                all_data.append(data)
            else:
                st.error(f"**{uploaded_file.name}**: {message}")
        
        progress_bar.progress(100)
        status_text.text("Processing complete!")
        
        if not all_data:
            st.error("❌ No valid data found in any uploaded files.")
            return
        
        # Combine all data
        st.markdown("---")
        st.subheader("📊 Generating Summary...")
        
        with st.spinner("Merging and aggregating data..."):
            combined_df = pd.concat(all_data, ignore_index=True)
            st.info(f"Combined: **{len(combined_df):,}** total rows from {len(all_data)} file(s)")
            
            # Aggregate
            summary = aggregate_data(combined_df)
        
        st.success(f"✅ Summary generated: **{len(summary):,}** unique accounts")
        
        # Store in session state
        st.session_state['merge_summary'] = summary
        st.session_state['merge_combined'] = combined_df
    
    # Show results if available
    if 'merge_summary' in st.session_state:
        summary = st.session_state['merge_summary']
        
        st.markdown("---")
        st.subheader("📋 Summary Results")
        
        # Stats
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Unique Accounts", f"{len(summary):,}")
        with col2:
            total_amount = summary['Transaction Amount'].sum()
            st.metric("Total Amount", f"₹{total_amount:,.2f}")
        with col3:
            total_disputed = summary['Disputed Amount'].sum()
            st.metric("Total Disputed", f"₹{total_disputed:,.2f}")
        with col4:
            total_txns = summary['Transaction Count'].sum()
            st.metric("Total Transactions", f"{total_txns:,}")
        
        # Preview
        with st.expander("📋 Preview Summary (First 100 rows)", expanded=True):
            display_df = summary.head(100).copy()
            display_df['Transaction Amount'] = display_df['Transaction Amount'].apply(lambda x: f"₹{x:,.2f}")
            display_df['Disputed Amount'] = display_df['Disputed Amount'].apply(lambda x: f"₹{x:,.2f}")
            st.dataframe(display_df, use_container_width=True)
        
        st.markdown("---")
        st.subheader("📥 Download")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Excel download
            buffer = io.BytesIO()
            MAX_ROWS = 1_048_576
            MAX_DATA_ROWS = MAX_ROWS - 1
            
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                if len(summary) > MAX_DATA_ROWS:
                    st.warning("⚠️ Data too large for single sheet. Splitting...")
                    for i in range(0, len(summary), MAX_DATA_ROWS):
                        chunk = summary.iloc[i:i + MAX_DATA_ROWS]
                        chunk.to_excel(writer, sheet_name=f"Summary_Part_{(i // MAX_DATA_ROWS) + 1}", index=False)
                else:
                    summary.to_excel(writer, sheet_name="Summary", index=False)
            
            buffer.seek(0)
            st.download_button(
                label="📊 Download Excel",
                data=buffer,
                file_name="merged_summary.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        with col2:
            # CSV download
            csv_data = summary.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📄 Download CSV",
                data=csv_data,
                file_name="merged_summary.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        # Clear button
        st.markdown("---")
        if st.button("🔄 Clear & Start Over", use_container_width=True):
            if 'merge_summary' in st.session_state:
                del st.session_state['merge_summary']
            if 'merge_combined' in st.session_state:
                del st.session_state['merge_combined']
            st.rerun()
