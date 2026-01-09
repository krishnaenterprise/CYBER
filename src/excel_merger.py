"""
Excel Merger Module.

Simple tool to merge multiple Excel/CSV files into one combined file.
"""
import streamlit as st
import pandas as pd
from io import BytesIO
from typing import List, Tuple


def generate_merged_excel(df: pd.DataFrame) -> bytes:
    """Generate Excel file bytes from DataFrame."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Merged Data')
    return output.getvalue()


def generate_merged_csv(df: pd.DataFrame) -> bytes:
    """Generate CSV file bytes from DataFrame."""
    return df.to_csv(index=False).encode('utf-8')


def read_file(uploaded_file) -> pd.DataFrame:
    """Read uploaded Excel/CSV file."""
    filename = uploaded_file.name.lower()
    if filename.endswith('.csv'):
        return pd.read_csv(uploaded_file)
    else:
        return pd.read_excel(uploaded_file)


def render_excel_merger_page():
    """Render the Excel merger page."""
    st.title("📎 Merge Excel Files")
    st.markdown("""
    Upload multiple Excel/CSV files and merge them into one file.
    - All files should have the **same column structure**
    - Files will be combined row by row
    - Download the merged file as Excel or CSV
    """)
    
    st.markdown("---")
    
    # Initialize session state
    if 'merger_files' not in st.session_state:
        st.session_state.merger_files = []  # List of (filename, dataframe) tuples
    if 'merger_counter' not in st.session_state:
        st.session_state.merger_counter = 0
    
    # Two columns layout
    col_upload, col_list = st.columns([1, 1])
    
    with col_upload:
        st.subheader("➕ Add Files")
        
        # File uploader
        uploaded_file = st.file_uploader(
            "Upload Excel/CSV file",
            type=['xlsx', 'xls', 'csv'],
            key=f"merger_uploader_{st.session_state.merger_counter}",
            help="Add files one by one. Each file will be added to the list."
        )
        
        # Add file button
        if uploaded_file is not None:
            # Check if already exists
            existing_names = [name for name, _ in st.session_state.merger_files]
            
            if uploaded_file.name in existing_names:
                st.warning(f"⚠️ '{uploaded_file.name}' already added!")
            else:
                if st.button("➕ Add This File", type="primary", use_container_width=True):
                    try:
                        df = read_file(uploaded_file)
                        st.session_state.merger_files.append((uploaded_file.name, df))
                        st.session_state.merger_counter += 1
                        st.success(f"✅ Added: {uploaded_file.name}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
    
    with col_list:
        st.subheader(f"📁 Files List ({len(st.session_state.merger_files)})")
        
        if st.session_state.merger_files:
            for i, (filename, df) in enumerate(st.session_state.merger_files):
                file_col, rows_col, remove_col = st.columns([2, 1, 0.5])
                with file_col:
                    st.text(f"{i+1}. {filename[:25]}..." if len(filename) > 25 else f"{i+1}. {filename}")
                with rows_col:
                    st.text(f"{len(df)} rows")
                with remove_col:
                    if st.button("❌", key=f"rm_{i}_{st.session_state.merger_counter}"):
                        st.session_state.merger_files.pop(i)
                        st.rerun()
            
            st.markdown("---")
            if st.button("🗑️ Clear All", use_container_width=True):
                st.session_state.merger_files = []
                st.session_state.merger_counter += 1
                st.rerun()
        else:
            st.info("No files added yet")
    
    # Show merged preview and download
    if len(st.session_state.merger_files) >= 2:
        st.markdown("---")
        st.subheader("📊 Merged Data")
        
        # Combine all dataframes
        try:
            combined_df = pd.concat(
                [df for _, df in st.session_state.merger_files], 
                ignore_index=True
            )
            
            # Stats
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Files", len(st.session_state.merger_files))
            with col2:
                st.metric("Total Rows", len(combined_df))
            with col3:
                st.metric("Columns", len(combined_df.columns))
            
            # Preview
            with st.expander("📋 Preview Merged Data", expanded=False):
                st.dataframe(combined_df.head(20), use_container_width=True)
            
            # Download buttons
            st.markdown("### ⬇️ Download Merged File")
            
            download_col1, download_col2 = st.columns(2)
            
            with download_col1:
                excel_bytes = generate_merged_excel(combined_df)
                st.download_button(
                    label=f"📊 Download Excel ({len(combined_df)} rows)",
                    data=excel_bytes,
                    file_name="merged_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary"
                )
            
            with download_col2:
                csv_bytes = generate_merged_csv(combined_df)
                st.download_button(
                    label=f"📄 Download CSV ({len(combined_df)} rows)",
                    data=csv_bytes,
                    file_name="merged_data.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                
        except Exception as e:
            st.error(f"❌ Error merging files: {str(e)}")
            st.info("Make sure all files have compatible column structures.")
    
    elif len(st.session_state.merger_files) == 1:
        st.markdown("---")
        st.info("📤 Add at least 2 files to merge them.")
