"""
Ticker directory model representing the relationship between CIK and ticker.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum

from ..exceptions import ValidationError


class TickerDirectoryStatus(str, Enum):
    """Enum for ticker directory status values."""
    ACTIVE = "active"
    INACTIVE = "inactive"


@dataclass
class TickerDirectory:
    """
    Represents a ticker directory entity with validation.
    
    Attributes:
        ticker: Stock ticker symbol
        cik: Central Index Key
        status: Status of the ticker (active, inactive)
        id: Auto-generated primary key
        created_at: Timestamp when the record was created
        last_updated_at: Timestamp when the record was last updated
    """
    ticker: str
    cik: int
    status: TickerDirectoryStatus
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    last_updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Clean and validate the ticker directory data after initialization."""
        # Clean ticker
        self.ticker = self.ticker.strip().upper()
        
        self.validate()
    
    def validate(self):
        """
        Validate ticker directory data.
        
        Raises:
            ValidationError: If validation fails
        """
        # Validate ticker
        if not self.ticker:
            raise ValidationError("ticker", self.ticker, "Ticker cannot be empty")
        
        if len(self.ticker) > 7:
            raise ValidationError("ticker", self.ticker, "Ticker cannot be longer than 7 characters")
        
        # Validate CIK
        if self.cik <= 0:
            raise ValidationError("cik", self.cik, "CIK must be a positive integer")
        
        # Validate ID if provided
        if self.id is not None and self.id <= 0:
            raise ValidationError("id", self.id, "ID must be a positive integer")
        
        # Validate status
        # Status validation is implicit through enum typing
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert ticker directory to dictionary.
        
        Returns:
            Dictionary representation of the ticker directory
        """
        return {
            'id': self.id,
            'ticker': self.ticker,
            'cik': self.cik,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_updated_at': self.last_updated_at.isoformat() if self.last_updated_at else None,
            'status': self.status.value
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TickerDirectory':
        """
        Create TickerDirectory instance from dictionary.
        
        Args:
            data: Dictionary containing ticker directory data
        
        Returns:
            TickerDirectory instance
        
        Raises:
            ValidationError: If data is invalid
        """
        # Convert status string to enum if needed
        status = data.get('status')
        if isinstance(status, str):
            status_enum = TickerDirectoryStatus(status)
        else:
            status_enum = status if status else TickerDirectoryStatus.ACTIVE
        
        # Parse timestamps if they're strings
        created_at = data.get('created_at')
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        
        last_updated_at = data.get('last_updated_at')
        if isinstance(last_updated_at, str):
            last_updated_at = datetime.fromisoformat(last_updated_at)
        
        return cls(
            ticker=str(data['ticker']),
            cik=int(data['cik']),
            status=status_enum,
            id=int(data['id']) if data.get('id') else None,
            created_at=created_at,
            last_updated_at=last_updated_at
        )
    
    @staticmethod
    def from_db_row(row: Any) -> 'TickerDirectory':
        """
        Create TickerDirectory instance from database row.
        
        Args:
            row: Database row tuple (cik, ticker, created_at, last_updated_at, status, id)
        
        Returns:
            TickerDirectory instance
        """
        return TickerDirectory(
            ticker=row[1],
            cik=row[0],
            status=TickerDirectoryStatus(row[4]),
            id=row[5],
            created_at=row[2],
            last_updated_at=row[3]
        )
