"""
Script to generate sample input and expected output files for documentation.

This script creates:
1. sample_input.xlsx - Sample input file with various column naming conventions
2. expected_output.xlsx - Expected output after processing
"""

import pandas as pd
from datetime import datetime
import os

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def create_sample_input():
    """
    Create a sample input Excel file with various column naming conventions
    and realistic fraud transaction data.
    """
    # Sample data with various column names that the system should recognize
    data = {
        "Sr No": list(range(1, 21)),
        "Acknowledgement No": [
            "ACK2024001", "ACK2024002", "ACK2024003", "ACK2024004", "ACK2024005",
            "ACK2024006", "ACK2024007", "ACK2024008", "ACK2024009", "ACK2024010",
            "ACK2024011", "ACK2024012", "ACK2024013", "ACK2024014", "ACK2024015",
            "ACK2024016", "ACK2024017", "ACK2024018", "ACK2024019", "ACK2024020"
        ],
        "Bank Account No": [
            "123456789012", "123456789012", "123456789012",  # Account 1 - 3 transactions
            "987654321098", "987654321098",                   # Account 2 - 2 transactions
            "555555555555", "555555555555", "555555555555", "555555555555",  # Account 3 - 4 transactions
            "111122223333", "111122223333", "111122223333",   # Account 4 - 3 transactions
            "444455556666",                                    # Account 5 - 1 transaction
            "777788889999", "777788889999", "777788889999", "777788889999", "777788889999",  # Account 6 - 5 transactions
            "999900001111", "999900001111"                     # Account 7 - 2 transactions
        ],
        "IFSC Code": [
            "SBIN0001234", "SBIN0001234", "SBIN0001234",
            "HDFC0005678", "HDFC0005678",
            "ICIC0009012", "ICIC0009012", "ICIC0009012", "ICIC0009012",
            "AXIS0003456", "AXIS0003456", "AXIS0003456",
            "PUNB0007890",
            "BARB0001234", "BARB0001234", "BARB0001234", "BARB0001234", "BARB0001234",
            "CNRB0005678", "CNRB0005678"
        ],
        "Address": [
            "123 Main Street, Mumbai, Maharashtra 400001",
            "123 Main Street, Mumbai, Maharashtra 400001",
            "123 Main Street, Mumbai, Maharashtra 400001",
            "456 Oak Avenue, Delhi, Delhi 110001",
            "456 Oak Avenue, Delhi, Delhi 110001",
            "789 Pine Road, Bangalore, Karnataka 560001",
            "789 Pine Road, Bangalore, Karnataka 560001",
            "789 Pine Road, Bangalore, Karnataka 560001",
            "789 Pine Road, Bangalore, Karnataka 560001",
            "321 Elm Street, Chennai, Tamil Nadu 600001",
            "321 Elm Street, Chennai, Tamil Nadu 600001",
            "321 Elm Street, Chennai, Tamil Nadu 600001",
            "654 Maple Lane, Kolkata, West Bengal 700001",
            "987 Cedar Drive, Hyderabad, Telangana 500001",
            "987 Cedar Drive, Hyderabad, Telangana 500001",
            "987 Cedar Drive, Hyderabad, Telangana 500001",
            "987 Cedar Drive, Hyderabad, Telangana 500001",
            "987 Cedar Drive, Hyderabad, Telangana 500001",
            "147 Birch Way, Pune, Maharashtra 411001",
            "147 Birch Way, Pune, Maharashtra 411001"
        ],
        "Amount": [
            15000.00, 25000.00, 10000.00,  # Account 1: Total 50,000
            75000.00, 50000.00,             # Account 2: Total 125,000
            20000.00, 30000.00, 15000.00, 35000.00,  # Account 3: Total 100,000
            45000.00, 55000.00, 40000.00,   # Account 4: Total 140,000
            8000.00,                         # Account 5: Total 8,000
            60000.00, 70000.00, 80000.00, 90000.00, 100000.00,  # Account 6: Total 400,000
            12000.00, 18000.00              # Account 7: Total 30,000
        ],
        "Disputed Amount": [
            15000.00, 25000.00, 10000.00,
            75000.00, 50000.00,
            20000.00, 30000.00, 15000.00, 35000.00,
            45000.00, 55000.00, 40000.00,
            8000.00,
            60000.00, 70000.00, 80000.00, 90000.00, 100000.00,
            12000.00, 18000.00
        ],
        "Bank Name": [
            "State Bank of India", "State Bank of India", "State Bank of India",
            "HDFC Bank", "HDFC Bank",
            "ICICI Bank", "ICICI Bank", "ICICI Bank", "ICICI Bank",
            "Axis Bank", "Axis Bank", "Axis Bank",
            "Punjab National Bank",
            "Bank of Baroda", "Bank of Baroda", "Bank of Baroda", "Bank of Baroda", "Bank of Baroda",
            "Canara Bank", "Canara Bank"
        ]
    }
    
    df = pd.DataFrame(data)
    
    # Save to Excel
    filepath = os.path.join(SCRIPT_DIR, "sample_input.xlsx")
    df.to_excel(filepath, index=False, engine='openpyxl')
    print(f"Created: {filepath}")
    
    return df


def create_expected_output():
    """
    Create the expected output Excel file after processing the sample input.
    This shows what the aggregated results should look like.
    """
    # Expected aggregated output (sorted by Total Amount descending)
    data = {
        "Fraudster Bank Account Number": [
            "777788889999",  # 400,000 - highest
            "111122223333",  # 140,000
            "987654321098",  # 125,000
            "555555555555",  # 100,000
            "123456789012",  # 50,000
            "999900001111",  # 30,000
            "444455556666"   # 8,000 - lowest
        ],
        "Bank Name": [
            "Bank of Baroda",
            "Axis Bank",
            "HDFC Bank",
            "ICICI Bank",
            "State Bank of India",
            "Canara Bank",
            "Punjab National Bank"
        ],
        "IFSC Code": [
            "BARB0001234",
            "AXIS0003456",
            "HDFC0005678",
            "ICIC0009012",
            "SBIN0001234",
            "CNRB0005678",
            "PUNB0007890"
        ],
        "Address": [
            "987 Cedar Drive, Hyderabad, Telangana 500001",
            "321 Elm Street, Chennai, Tamil Nadu 600001",
            "456 Oak Avenue, Delhi, Delhi 110001",
            "789 Pine Road, Bangalore, Karnataka 560001",
            "123 Main Street, Mumbai, Maharashtra 400001",
            "147 Birch Way, Pune, Maharashtra 411001",
            "654 Maple Lane, Kolkata, West Bengal 700001"
        ],
        "Total Transactions": [5, 3, 2, 4, 3, 2, 1],
        "All Acknowledgement Numbers": [
            "ACK2024014;ACK2024015;ACK2024016;ACK2024017;ACK2024018",
            "ACK2024010;ACK2024011;ACK2024012",
            "ACK2024004;ACK2024005",
            "ACK2024006;ACK2024007;ACK2024008;ACK2024009",
            "ACK2024001;ACK2024002;ACK2024003",
            "ACK2024019;ACK2024020",
            "ACK2024013"
        ],
        "Total Amount": [
            400000.00,
            140000.00,
            125000.00,
            100000.00,
            50000.00,
            30000.00,
            8000.00
        ],
        "Total Disputed Amount": [
            400000.00,
            140000.00,
            125000.00,
            100000.00,
            50000.00,
            30000.00,
            8000.00
        ],
        "Risk Score": [
            62.0,   # 5 transactions, 400k amount
            48.44,  # 3 transactions, 140k amount
            43.7,   # 2 transactions, 125k amount
            36.8,   # 4 transactions, 100k amount
            14.2,   # 3 transactions, 50k amount
            10.6,   # 2 transactions, 30k amount
            0.88    # 1 transaction, 8k amount
        ]
    }
    
    df = pd.DataFrame(data)
    
    # Save to Excel
    filepath = os.path.join(SCRIPT_DIR, "expected_output.xlsx")
    df.to_excel(filepath, index=False, engine='openpyxl')
    print(f"Created: {filepath}")
    
    return df


def create_sample_with_variant_columns():
    """
    Create a sample input file with alternative column naming conventions
    to demonstrate the fuzzy matching capability.
    """
    data = {
        "S.No": [1, 2, 3, 4, 5],
        "Ref No": ["REF001", "REF002", "REF003", "REF004", "REF005"],
        "Beneficiary Account": [
            "123456789012", "123456789012", "987654321098", "987654321098", "555555555555"
        ],
        "Bank Code": ["SBIN0001234", "SBIN0001234", "HDFC0005678", "HDFC0005678", "ICIC0009012"],
        "Location": [
            "Mumbai", "Mumbai", "Delhi", "Delhi", "Bangalore"
        ],
        "Txn Amount": [10000.00, 20000.00, 30000.00, 40000.00, 50000.00],
        "Claim Amount": [10000.00, 20000.00, 30000.00, 40000.00, 50000.00],
        "Beneficiary Bank": [
            "State Bank of India", "State Bank of India", 
            "HDFC Bank", "HDFC Bank", "ICICI Bank"
        ]
    }
    
    df = pd.DataFrame(data)
    
    # Save to Excel
    filepath = os.path.join(SCRIPT_DIR, "sample_input_variant_columns.xlsx")
    df.to_excel(filepath, index=False, engine='openpyxl')
    print(f"Created: {filepath}")
    
    return df


if __name__ == "__main__":
    print("Generating sample files for documentation...")
    print("-" * 50)
    
    create_sample_input()
    create_expected_output()
    create_sample_with_variant_columns()
    
    print("-" * 50)
    print("Done! Sample files created in the 'data' directory.")
