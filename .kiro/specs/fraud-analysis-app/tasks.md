# Implementation Plan: Cybercrime Fraud Analysis Web Application

## Overview

This implementation plan breaks down the fraud analysis web application into incremental coding tasks. Each task builds on previous work, with property-based tests validating correctness throughout. The application uses Python with Streamlit, pandas, and hypothesis for testing.

## Tasks

- [x] 1. Set up project structure and dependencies
  - Create project directory structure: `src/`, `tests/`, `data/`
  - Create `requirements.txt` with: streamlit, pandas, openpyxl, xlrd, rapidfuzz, hypothesis, pytest, pytest-cov, reportlab
  - Create `src/__init__.py` and `src/models.py` with dataclasses (ColumnMapping, ValidationResult, AggregatedAccount, ProcessingStats, ErrorResponse)
  - Create `conftest.py` with pytest fixtures and hypothesis strategies
  - _Requirements: All_

- [x] 2. Implement Upload Service
  - [x] 2.1 Create `src/upload_service.py` with UploadService class
    - Implement `validate_file()` for type and size validation
    - Implement `read_file()` to read Excel/CSV into DataFrame
    - Implement `get_preview()` to return first N rows
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_
  - [x] 2.2 Write property tests for file validation
    - **Property 1: File Type Validation**
    - **Property 2: File Size Validation**
    - **Validates: Requirements 1.1, 1.2**

- [x] 3. Implement Column Detector
  - [x] 3.1 Create `src/column_detector.py` with ColumnDetector class
    - Implement `normalize_header()` to strip whitespace, lowercase, remove special chars
    - Implement `calculate_similarity()` using rapidfuzz for fuzzy matching
    - Implement `detect_columns()` to map headers to column types
    - Define COLUMN_VARIANTS dictionary with all known variants
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.6-2.13_
  - [x] 3.2 Write property tests for header normalization
    - **Property 3: Header Normalization Idempotence**
    - **Validates: Requirements 2.2**
  - [x] 3.3 Write property tests for fuzzy matching
    - **Property 4: Fuzzy Matching Threshold Consistency**
    - **Property 5: Column Variant Recognition**
    - **Validates: Requirements 2.1, 2.6-2.13**

- [x] 4. Checkpoint - Core input handling complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement Data Processor
  - [x] 5.1 Create `src/data_processor.py` with DataProcessor class
    - Implement `remove_empty_rows()` to filter out completely empty rows
    - Implement `trim_whitespace()` to strip all string cells
    - Implement `standardize_account_number()` to remove spaces and dashes
    - Implement `parse_amount()` to handle currency symbols and commas
    - Implement `clean_dataframe()` to orchestrate all cleaning operations
    - _Requirements: 3.1, 3.2, 3.3, 3.6_
  - [x] 5.2 Write property tests for data cleaning
    - **Property 6: Empty Row Removal**
    - **Property 7: Whitespace Trimming**
    - **Property 8: Account Number Standardization**
    - **Property 11: Amount Parsing Correctness**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.6**

- [x] 6. Implement Validation Engine
  - [x] 6.1 Create `src/validation_engine.py` with ValidationEngine class
    - Implement `validate_account_number()` for 9-18 digit check
    - Implement `validate_ifsc_code()` for 11 alphanumeric char check
    - Implement `validate_amount()` for positive number check
    - Implement `check_duplicate_acknowledgements()` to find duplicates
    - Implement `validate_dataframe()` to run all validations
    - Implement `generate_quality_report()` for data quality metrics
    - Define error categories (Critical vs Warning)
    - _Requirements: 3.4, 3.5, 3.7, 3.8, 3.9, 3.10, 10.1-10.6_
  - [x] 6.2 Write property tests for field validation
    - **Property 9: Account Number Validation**
    - **Property 10: IFSC Code Validation**
    - **Property 12: Amount Validation**
    - **Property 13: Critical Data Flagging**
    - **Property 14: Duplicate Acknowledgement Detection**
    - **Property 28: Error Classification Consistency**
    - **Validates: Requirements 3.4, 3.5, 3.7, 3.8, 3.9, 10.1-10.6**

- [x] 7. Checkpoint - Data processing pipeline complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement Aggregation Engine
  - [x] 8.1 Create `src/aggregation_engine.py` with AggregationEngine class
    - Implement `get_most_common()` to find mode of a series
    - Implement `calculate_risk_score()` based on transactions and amount
    - Implement `aggregate_by_account()` to group and calculate all aggregates
    - Implement `sort_results()` for primary/secondary sorting
    - _Requirements: 4.1-4.9, 5.2, 5.3_
  - [x] 8.2 Write property tests for aggregation
    - **Property 15: Grouping Uniqueness Invariant**
    - **Property 16: Acknowledgement Number Consolidation**
    - **Property 17: Amount Sum Aggregation**
    - **Property 18: Mode Aggregation for Categorical Fields**
    - **Property 19: Transaction Count Accuracy**
    - **Property 20: Risk Score Determinism**
    - **Property 21: Output Sorting Order**
    - **Validates: Requirements 4.1-4.9, 5.2, 5.3**

- [x] 9. Implement Report Generator
  - [x] 9.1 Create `src/report_generator.py` with ReportGenerator class
    - Implement `generate_excel()` to create summary Excel file
    - Implement `generate_csv()` to create summary CSV file
    - Implement `generate_audit_log()` to create processing log
    - _Requirements: 5.1, 5.4, 5.5, 9.1-9.6_
  - [x] 9.2 Write property tests for export round-trips
    - **Property 22: Excel Export Round-Trip**
    - **Property 23: CSV Export Round-Trip**
    - **Property 27: Audit Log Completeness**
    - **Validates: Requirements 5.1, 5.4, 5.5, 9.1-9.6**
  - [x] 9.3 Implement PDF report generation
    - Implement `generate_pdf()` using reportlab
    - Include summary statistics, top 20 accounts table, data quality metrics
    - _Requirements: 5.6_

- [x] 10. Checkpoint - Core processing complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Implement Dashboard Statistics and Filters
  - [x] 11.1 Create `src/dashboard.py` with Dashboard class
    - Implement `calculate_statistics()` for summary stats
    - Implement `search_accounts()` for account number search
    - Implement `filter_by_min_transactions()` for transaction count filter
    - Implement `filter_by_min_amount()` for amount filter
    - _Requirements: 6.1-6.9_
  - [x] 11.2 Write property tests for statistics and filters
    - **Property 24: Statistics Calculation Consistency**
    - **Property 25: Search Filter Correctness**
    - **Property 26: Minimum Filter Correctness**
    - **Validates: Requirements 6.1-6.9**

- [x] 12. Implement Session Manager
  - [x] 12.1 Create `src/session_manager.py` with SessionManager class
    - Implement `create_session()` with unique ID generation
    - Implement `validate_session()` for session validity check
    - Implement `store_data()` and `get_data()` for in-memory storage
    - Implement `cleanup_session()` to delete all session data
    - Implement `check_timeout()` for 30-minute timeout
    - _Requirements: 8.1-8.6_

- [x] 13. Build Streamlit Web Interface
  - [x] 13.1 Create `src/app.py` with main Streamlit application
    - Page 1: Upload page with drag-and-drop, file validation, preview
    - Page 2: Column mapping with auto-suggestions and manual override
    - Page 3: Processing page with progress bar and real-time logging
    - Page 4: Results dashboard with statistics, downloads, search/filter
    - _Requirements: 1.4, 2.4, 2.5, 6.1-6.9, 7.1-7.5_
  - [x] 13.2 Add authentication and security features
    - Implement password protection for application access
    - Add session timeout handling
    - Display data handling disclaimer
    - _Requirements: 8.3, 8.4, 8.6_

- [x] 14. Integration and Final Testing
  - [x] 14.1 Create integration tests
    - End-to-end test: upload → process → download
    - Test with various Excel formats and column naming conventions
    - Test with 10, 100, 1000, 10000+ rows
    - Test missing data scenarios
    - Test multi-sheet Excel files (use first sheet)
    - _Requirements: All_
  - [x] 14.2 Create sample input/output files for documentation
    - Create sample input Excel with various column names
    - Create expected output Excel for verification
    - _Requirements: Documentation_

- [x] 15. Final Checkpoint
  - Ensure all tests pass, ask the user if questions arise.
  - Verify all requirements are covered
  - Run full test suite with coverage report

## Notes

- All tasks including property tests are required for comprehensive coverage
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- The application uses Streamlit for rapid web UI development
- All data processing happens in-memory for security
