"""
Data models for the Fraud Analysis Application.

Contains dataclasses for column mapping, validation results, aggregated accounts,
processing statistics, and error responses.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class ErrorCategory(Enum):
    """Classification of errors as critical or warning."""
    CRITICAL = "critical"  # Blocks processing
    WARNING = "warning"    # Flags but continues


@dataclass
class ColumnMapping:
    """Mapping of detected columns to their standardized names."""
    serial_number: Optional[str] = None
    acknowledgement_number: Optional[str] = None
    bank_account_number: Optional[str] = None  # Required
    ifsc_code: Optional[str] = None
    address: Optional[str] = None
    amount: Optional[str] = None  # Required
    disputed_amount: Optional[str] = None
    bank_name: Optional[str] = None
    district: Optional[str] = None
    state: Optional[str] = None
    confidence_scores: Dict[str, float] = field(default_factory=dict)
    ambiguous_mappings: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of data validation operations."""
    is_valid: bool
    critical_errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    flagged_rows: List[int] = field(default_factory=list)
    quality_report: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AggregatedAccount:
    """Aggregated transaction data for a single fraudster account."""
    account_number: str
    bank_name: str
    ifsc_code: str
    address: str
    district: str
    state: str
    total_transactions: int
    acknowledgement_numbers: str  # semicolon-separated
    total_amount: float
    total_disputed_amount: float
    risk_score: float


@dataclass
class ProcessingStats:
    """Statistics from processing a transaction file."""
    total_input_rows: int
    rows_processed: int
    rows_with_errors: int
    unique_accounts: int
    total_fraud_amount: float
    total_disputed_amount: float
    average_amount_per_account: float
    top_accounts_by_amount: List[AggregatedAccount] = field(default_factory=list)
    processing_timestamp: datetime = field(default_factory=datetime.now)
    input_filename: str = ""


@dataclass
class SessionInfo:
    """Information about a user session."""
    session_id: str
    created_at: datetime
    last_activity: datetime
    input_filename: Optional[str] = None
    user_id: Optional[str] = None


@dataclass
class ErrorResponse:
    """Structured error response for validation and processing errors."""
    category: ErrorCategory
    code: str
    message: str
    row_number: Optional[int] = None
    field_name: Optional[str] = None
    original_value: Optional[str] = None
