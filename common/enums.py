"""
Enumerations for MLP pipeline
"""

from enum import Enum, auto


class DataSource(Enum):
    """
    Data source enumeration
    """
    SEC_ADV = auto()


class OutputFormat(Enum):
    """
    Output format enumeration
    """
    CSV = "CSV"
    EXCEL = "CSV"
    JSON = "JSON"
    SQLITE = "SQLITE"
