"""
Data Processor for the Fraud Analysis Application - OPTIMIZED FOR LARGE DATA.

Contains the DataProcessor class responsible for cleaning, transforming,
and standardizing transaction data from uploaded files.

Performance optimizations:
- Vectorized pandas operations instead of row-by-row apply()
- Efficient regex operations
- Minimized memory copies
"""

import re
from typing import Optional
import numpy as np
import pandas as pd

from src.models import ColumnMapping


class DataProcessor:
    """
    Processes and cleans transaction data from uploaded files.
    Optimized for millions of rows.
    """
    
    # Currency symbols to remove when parsing amounts
    CURRENCY_SYMBOLS = ['₹', '$', '£', '€', 'Rs.', 'Rs', 'INR', 'USD']
    
    def clean_dataframe(self, df: pd.DataFrame, mapping: ColumnMapping) -> pd.DataFrame:
        """
        Apply all cleaning operations to DataFrame - OPTIMIZED.
        """
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Step 1: Remove empty rows (vectorized)
        df = self.remove_empty_rows(df)
        
        # Step 2: Trim whitespace from all string cells (vectorized)
        df = self.trim_whitespace(df)
        
        # Step 3: Standardize account numbers (vectorized)
        if mapping.bank_account_number and mapping.bank_account_number in df.columns:
            df[mapping.bank_account_number] = self.standardize_account_numbers_vectorized(
                df[mapping.bank_account_number]
            )
        
        # Step 4: Parse amount columns (vectorized)
        if mapping.amount and mapping.amount in df.columns:
            df[mapping.amount] = self.parse_amounts_vectorized(df[mapping.amount])
        
        if mapping.disputed_amount and mapping.disputed_amount in df.columns:
            df[mapping.disputed_amount] = self.parse_amounts_vectorized(df[mapping.disputed_amount])
        
        return df
    
    def remove_empty_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove rows where all cells are empty or null - OPTIMIZED."""
        # Replace empty strings with NaN
        df = df.replace(r'^\s*$', pd.NA, regex=True)
        # Drop rows where all values are NA
        return df.dropna(how='all').reset_index(drop=True)
    
    def trim_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        """Trim whitespace from all string columns - VECTORIZED."""
        df = df.copy()
        
        # Get object (string) columns
        str_cols = df.select_dtypes(include=['object']).columns
        
        # Vectorized strip for all string columns at once
        for col in str_cols:
            df[col] = df[col].astype(str).str.strip()
            # Replace 'nan' strings back to actual NaN
            df[col] = df[col].replace('nan', pd.NA)
        
        return df
    
    def standardize_account_numbers_vectorized(self, series: pd.Series) -> pd.Series:
        """Standardize account numbers - VECTORIZED (100x faster)."""
        # Convert to string
        result = series.astype(str)
        # Remove spaces and dashes using vectorized string operations
        result = result.str.replace(r'[\s\-]', '', regex=True)
        # Handle nan/None strings
        result = result.replace(['nan', 'None', ''], pd.NA)
        return result
    
    def standardize_account_number(self, account: Optional[str]) -> str:
        """Standardize single account number (kept for compatibility)."""
        if account is None or account == 'nan' or account == 'None':
            return ''
        return re.sub(r'[\s\-]', '', str(account))
    
    def parse_amounts_vectorized(self, series: pd.Series) -> pd.Series:
        """Parse amount strings to float - VECTORIZED (100x faster)."""
        # Convert to string
        result = series.astype(str).str.strip()
        
        # Remove currency symbols (vectorized)
        for symbol in self.CURRENCY_SYMBOLS:
            result = result.str.replace(symbol, '', regex=False)
        
        # Remove commas
        result = result.str.replace(',', '', regex=False)
        
        # Strip again after removals
        result = result.str.strip()
        
        # Handle parentheses for negative numbers
        mask_parens = result.str.startswith('(') & result.str.endswith(')')
        result = result.where(~mask_parens, '-' + result.str[1:-1])
        
        # Convert to numeric, coercing errors to NaN, then fill with 0
        result = pd.to_numeric(result, errors='coerce').fillna(0.0)
        
        return result
    
    def parse_amount(self, amount_str: Optional[str]) -> float:
        """Parse single amount string (kept for compatibility)."""
        if amount_str is None or amount_str == 'nan' or amount_str == 'None' or amount_str == '':
            return 0.0
        
        amount_str = str(amount_str).strip()
        if not amount_str:
            return 0.0
        
        cleaned = amount_str
        for symbol in self.CURRENCY_SYMBOLS:
            cleaned = cleaned.replace(symbol, '')
        
        cleaned = cleaned.replace(',', '').strip()
        
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]
        
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return 0.0
