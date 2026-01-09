# Sample Data Files

This directory contains sample input and output files for the Fraud Analysis Application.

## Files

### sample_input.xlsx
A sample input Excel file containing 20 fraud transaction records with the following columns:
- **Sr No**: Serial number
- **Acknowledgement No**: Unique acknowledgement number for each transaction
- **Bank Account No**: Fraudster's bank account number (9-18 digits)
- **IFSC Code**: Bank IFSC code (11 alphanumeric characters)
- **Address**: Fraudster's address
- **Amount**: Transaction amount
- **Disputed Amount**: Disputed/claimed amount
- **Bank Name**: Name of the bank

The sample data includes:
- 7 unique fraudster accounts
- 20 total transactions
- Various transaction amounts ranging from ₹8,000 to ₹100,000
- Multiple transactions per account to demonstrate aggregation

### expected_output.xlsx
The expected output after processing `sample_input.xlsx`. This file shows:
- Aggregated results grouped by fraudster bank account
- Sorted by Total Amount (descending)
- Consolidated acknowledgement numbers (semicolon-separated)
- Calculated risk scores

Output columns:
- **Fraudster Bank Account Number**: The account number
- **Bank Name**: Most common bank name for the account
- **IFSC Code**: Most common IFSC code
- **Address**: Most common address
- **Total Transactions**: Count of transactions
- **All Acknowledgement Numbers**: Semicolon-separated list
- **Total Amount**: Sum of all transaction amounts
- **Total Disputed Amount**: Sum of all disputed amounts
- **Risk Score**: Calculated risk score (0-100)

### sample_input_variant_columns.xlsx
A sample input file demonstrating alternative column naming conventions:
- **S.No** (instead of "Sr No")
- **Ref No** (instead of "Acknowledgement No")
- **Beneficiary Account** (instead of "Bank Account No")
- **Bank Code** (instead of "IFSC Code")
- **Location** (instead of "Address")
- **Txn Amount** (instead of "Amount")
- **Claim Amount** (instead of "Disputed Amount")
- **Beneficiary Bank** (instead of "Bank Name")

This file demonstrates the fuzzy column detection capability of the application.

## Regenerating Sample Files

To regenerate the sample files, run:

```bash
python data/sample_input.py
```

## Usage

1. Upload `sample_input.xlsx` or `sample_input_variant_columns.xlsx` to the application
2. The system will automatically detect column mappings
3. Process the data to generate aggregated results
4. Compare the output with `expected_output.xlsx` for verification
