# Requirements Document

## Introduction

A Python web application for law enforcement cybercrime departments to analyze and consolidate fraud transaction data from Excel files. The system identifies patterns by grouping transactions by fraudster bank accounts and provides actionable summaries for legal proceedings.

## Glossary

- **Upload_Service**: Component responsible for accepting and validating file uploads
- **Column_Detector**: Component that uses fuzzy matching to identify and map column headers
- **Data_Processor**: Component that cleans, validates, and transforms raw transaction data
- **Aggregation_Engine**: Component that groups transactions by account and calculates summaries
- **Report_Generator**: Component that creates Excel, CSV, and PDF output files
- **Session_Manager**: Component that handles user sessions and data lifecycle
- **Validation_Engine**: Component that validates data integrity and formats

## Requirements

### Requirement 1: File Upload and Validation

**User Story:** As a cybercrime analyst, I want to upload Excel files containing fraud transaction data, so that I can process and analyze them.

#### Acceptance Criteria

1. WHEN a user uploads a file, THE Upload_Service SHALL accept Excel files (.xlsx, .xls) and CSV files
2. WHEN a file exceeds 50MB, THE Upload_Service SHALL reject the upload and display an error message
3. WHEN a file is uploaded, THE Upload_Service SHALL validate the file format before processing
4. WHEN a valid file is uploaded, THE Upload_Service SHALL display a preview of the first 10 rows
5. IF a file is corrupted or unreadable, THEN THE Upload_Service SHALL display a descriptive error message

### Requirement 2: Flexible Column Header Detection

**User Story:** As a cybercrime analyst, I want the system to automatically detect column headers with various naming conventions, so that I can process files from different sources without manual configuration.

#### Acceptance Criteria

1. WHEN processing headers, THE Column_Detector SHALL use fuzzy matching with 80% or higher similarity threshold
2. WHEN processing headers, THE Column_Detector SHALL strip whitespace, convert to lowercase, and remove special characters
3. WHEN multiple potential matches are found for a column, THE Column_Detector SHALL flag for user confirmation
4. WHEN a column is detected, THE Column_Detector SHALL display confidence scores for automatic suggestions
5. WHEN automatic detection fails, THE Column_Detector SHALL allow manual column mapping override
6. THE Column_Detector SHALL recognize Serial Number variants including: sr no, sr.no, serial no, s.no, sno, serial number, #
7. THE Column_Detector SHALL recognize Acknowledgement Number variants including: acknowledgement no, ack no, ackno, ack, acknowledgment no, acknowledgement number, acknowledgment number, ref no, reference no
8. THE Column_Detector SHALL recognize Bank Account Number variants including: bank account no, bank ac no, bank a/c no, ac no, a/c no, account no, account number, bank account number, beneficiary account, beneficiary ac
9. THE Column_Detector SHALL recognize IFSC Code variants including: ifsc code, ifsc, bank code
10. THE Column_Detector SHALL recognize Address variants including: address, beneficiary address, account holder address, location
11. THE Column_Detector SHALL recognize Amount variants including: amount, transaction amount, txn amount, transfer amount, fraud amount
12. THE Column_Detector SHALL recognize Disputed Amount variants including: disputed amount, disputed, claim amount, disputed amt, chargeback amount
13. THE Column_Detector SHALL recognize Bank Name variants including: bank name, bank, beneficiary bank, receiving bank

### Requirement 3: Data Cleaning and Validation

**User Story:** As a cybercrime analyst, I want the system to clean and validate transaction data, so that I can ensure data quality for legal proceedings.

#### Acceptance Criteria

1. WHEN processing data, THE Data_Processor SHALL remove completely empty rows
2. WHEN processing data, THE Data_Processor SHALL trim whitespace from all cells
3. WHEN processing bank account numbers, THE Data_Processor SHALL standardize by removing spaces and dashes
4. WHEN validating bank account numbers, THE Validation_Engine SHALL verify they contain 9-18 digits
5. WHEN validating IFSC codes, THE Validation_Engine SHALL verify exactly 11 alphanumeric characters
6. WHEN processing amounts, THE Data_Processor SHALL convert to float and handle currency symbols and commas
7. WHEN validating amounts, THE Validation_Engine SHALL verify they are positive numbers
8. IF a row has critical missing data (account number or amount), THEN THE Data_Processor SHALL flag the row
9. IF the same acknowledgement number appears multiple times, THEN THE Validation_Engine SHALL generate a warning
10. WHEN validation completes, THE Validation_Engine SHALL generate a data quality report

### Requirement 4: Transaction Grouping and Aggregation

**User Story:** As a cybercrime analyst, I want transactions grouped by fraudster bank account, so that I can identify fraud patterns and networks.

#### Acceptance Criteria

1. WHEN grouping transactions, THE Aggregation_Engine SHALL use Bank Account Number as the primary key
2. FOR each unique account number, THE Aggregation_Engine SHALL consolidate all acknowledgement numbers as a semicolon-separated list
3. FOR each unique account number, THE Aggregation_Engine SHALL calculate the sum of all transaction amounts
4. FOR each unique account number, THE Aggregation_Engine SHALL calculate the sum of all disputed amounts
5. FOR each unique account number, THE Aggregation_Engine SHALL determine the most common bank name
6. FOR each unique account number, THE Aggregation_Engine SHALL determine the most common IFSC code
7. FOR each unique account number, THE Aggregation_Engine SHALL determine the most common address
8. FOR each unique account number, THE Aggregation_Engine SHALL count the total number of transactions
9. THE Aggregation_Engine SHALL calculate a Risk Score based on number of transactions and total amount

### Requirement 5: Output Generation

**User Story:** As a cybercrime analyst, I want to download consolidated results in multiple formats, so that I can use them for investigations and legal proceedings.

#### Acceptance Criteria

1. THE Report_Generator SHALL create a summary Excel file with columns: Fraudster Bank Account Number, Bank Name, IFSC Code, Address, Total Transactions, All Acknowledgement Numbers, Total Amount, Total Disputed Amount, Risk Score
2. WHEN generating output, THE Report_Generator SHALL sort primarily by Total Amount descending
3. WHEN generating output, THE Report_Generator SHALL sort secondarily by Total Transactions descending
4. THE Report_Generator SHALL provide Excel export option
5. THE Report_Generator SHALL provide CSV export option
6. THE Report_Generator SHALL provide PDF report option with summary statistics, top 20 fraudster accounts table, data quality metrics, and processing timestamp

### Requirement 6: Results Dashboard

**User Story:** As a cybercrime analyst, I want to view summary statistics and search results, so that I can quickly identify key fraud patterns.

#### Acceptance Criteria

1. WHEN processing completes, THE Dashboard SHALL display total input transactions count
2. WHEN processing completes, THE Dashboard SHALL display unique fraudster accounts identified
3. WHEN processing completes, THE Dashboard SHALL display total fraud amount
4. WHEN processing completes, THE Dashboard SHALL display average amount per account
5. WHEN processing completes, THE Dashboard SHALL display top 10 accounts by amount
6. THE Dashboard SHALL allow searching for specific account numbers in results
7. THE Dashboard SHALL allow filtering by minimum transaction count
8. THE Dashboard SHALL allow filtering by minimum amount
9. THE Dashboard SHALL provide option to view flagged or error rows

### Requirement 7: Processing Feedback

**User Story:** As a cybercrime analyst, I want to see real-time processing progress, so that I can monitor the analysis status.

#### Acceptance Criteria

1. WHILE processing data, THE Data_Processor SHALL display a progress bar with status updates
2. WHILE processing data, THE Data_Processor SHALL show rows processed count
3. WHILE processing data, THE Data_Processor SHALL show accounts identified count
4. WHILE processing data, THE Data_Processor SHALL show errors found count
5. WHILE processing data, THE Data_Processor SHALL provide real-time error logging

### Requirement 8: Session and Data Security

**User Story:** As a cybercrime analyst, I want my data handled securely, so that sensitive investigation data is protected.

#### Acceptance Criteria

1. THE Session_Manager SHALL ensure no data is stored on server after session ends
2. THE Session_Manager SHALL perform all processing in memory
3. THE Session_Manager SHALL timeout sessions after 30 minutes of inactivity
4. THE Session_Manager SHALL require password protection for application access
5. WHEN a session ends, THE Session_Manager SHALL permanently delete all uploaded files
6. THE Application SHALL display a clear disclaimer about data handling

### Requirement 9: Audit Trail

**User Story:** As a cybercrime analyst, I want an audit trail of processing activities, so that I can document the analysis for legal proceedings.

#### Acceptance Criteria

1. FOR each processing session, THE Report_Generator SHALL create a log file
2. THE audit log SHALL include timestamp of processing
3. THE audit log SHALL include input file name
4. THE audit log SHALL include rows processed count
5. THE audit log SHALL include errors encountered
6. THE audit log SHALL be downloadable with results

### Requirement 10: Error Handling

**User Story:** As a cybercrime analyst, I want clear error messages and graceful error handling, so that I can understand and resolve issues.

#### Acceptance Criteria

1. IF no account number column is found, THEN THE Validation_Engine SHALL block processing and display a critical error
2. IF no amount column is found, THEN THE Validation_Engine SHALL block processing and display a critical error
3. IF IFSC code is missing, THEN THE Validation_Engine SHALL flag as warning and continue processing
4. IF address is missing, THEN THE Validation_Engine SHALL flag as warning and continue processing
5. IF amount format is invalid, THEN THE Data_Processor SHALL use 0 and flag as warning
6. THE Validation_Engine SHALL categorize errors as Critical (block processing) or Warnings (flag but continue)
