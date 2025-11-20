"""
Data Layer Package for Stock Information Website

This package provides a comprehensive data access layer for stock-related operations,
including database connection management, models, and repositories with full CRUD capabilities.
"""

from .models.ticker_summary import TickerSummary
from .models.ticker_overview import TickerOverview
from .repositories.ticker_summary_repository import TickerSummaryRepository
from .repositories.ticker_overview_repository import TickerOverviewRepository
from .database.connection_manager import DatabaseConnectionManager
from .exceptions import (
    DataLayerError,
    DatabaseConnectionError,
    DatabaseQueryError,
    StockNotFoundError,
    DuplicateStockError,
    ValidationError
)

__version__ = "1.0.0"
__all__ = [
    "TickerSummary",
    "TickerOverview",
    "TickerSummaryRepository",
    "TickerOverviewRepository",
    "DatabaseConnectionManager",
    "DataLayerError",
    "DatabaseConnectionError",
    "DatabaseQueryError",
    "StockNotFoundError",
    "DuplicateStockError",
    "ValidationError",
]