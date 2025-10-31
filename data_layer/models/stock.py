"""
Stock model representing a stock entity.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import re

from ..exceptions import ValidationError


@dataclass
class Stock:
    """
    Represents a stock entity with validation.
    """
    symbol: str
    company: Optional[str] = None
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    last_updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate the stock data after initialization."""
        self.validate()
    
    def validate(self):
        """
        Validate stock data.
        
        Raises:
            ValidationError: If validation fails
        """
        # Validate symbol
        if not self.symbol:
            raise ValidationError("symbol", self.symbol, "Symbol cannot be empty")
        
        # Clean and validate symbol format
        self.symbol = self.symbol.strip().upper()
        
        if len(self.symbol) == 0:
            raise ValidationError("symbol", self.symbol, "Symbol cannot be empty after cleaning")
        
        if len(self.symbol) > 20:
            raise ValidationError("symbol", self.symbol, "Symbol cannot be longer than 20 characters")
        
        # Basic symbol format validation (alphanumeric, dots, hyphens allowed)
        if not re.match(r'^[A-Z0-9.-]+$', self.symbol):
            raise ValidationError(
                "symbol", 
                self.symbol, 
                "Symbol can only contain uppercase letters, numbers, dots, and hyphens"
            )
        
        # Validate company name if provided
        if self.company is not None:
            self.company = self.company.strip()
            if len(self.company) == 0:
                self.company = None  # Convert empty string to None
            elif len(self.company) > 255:
                raise ValidationError("company", self.company, "Company name cannot be longer than 255 characters")
        
    
    def to_dict(self, include_timestamps: bool = True) -> Dict[str, Any]:
        """
        Convert stock to dictionary.
        
        Args:
            include_timestamps: Whether to include created_at and last_updated_at
        
        Returns:
            Dictionary representation of the stock
        """
        result: Dict[str, Any] = {
            'id': self.id,
            'symbol': self.symbol,
            'company': self.company
        }
        
        if include_timestamps:
            result.update({
                'created_at': self.created_at.isoformat() if self.created_at else None,
                'last_updated_at': self.last_updated_at.isoformat() if self.last_updated_at else None
            })
        
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Stock':
        """
        Create Stock instance from dictionary.
        
        Args:
            data: Dictionary containing stock data
        
        Returns:
            Stock instance
        """
        # Handle datetime conversion
        created_at = data.get('created_at')
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        
        last_updated_at = data.get('last_updated_at')
        if isinstance(last_updated_at, str):
            last_updated_at = datetime.fromisoformat(last_updated_at.replace('Z', '+00:00'))
        
        return cls(
            id=data.get('id'),
            symbol=data['symbol'],
            company=data.get('company'),
            created_at=created_at,
            last_updated_at=last_updated_at
        )
    
    @classmethod
    def from_db_row(cls, row: tuple[Any, ...], columns: list[str]) -> 'Stock':
        """
        Create Stock instance from database row.
        
        Args:
            row: Database row tuple
            columns: List of column names corresponding to row values
        
        Returns:
            Stock instance
        """
        data = dict(zip(columns, row))
        return cls.from_dict(data)
    
    def update_from_dict(self, data: Dict[str, Any], validate: bool = True):
        """
        Update stock attributes from dictionary.
        
        Args:
            data: Dictionary containing updated values
            validate: Whether to validate after updating
        """
        for key, value in data.items():
            if hasattr(self, key) and key not in ['id', 'symbol']:  # Don't allow ID or symbol updates (symbol is now PK)
                setattr(self, key, value)
        
        if validate:
            self.validate()
    
    def __str__(self) -> str:
        """String representation of the stock."""
        company_part = f" ({self.company})" if self.company else ""
        return f"{self.symbol}{company_part}"
    
    def __repr__(self) -> str:
        """Detailed string representation of the stock."""
        return (f"Stock(id={self.id}, symbol='{self.symbol}', "
                f"company='{self.company}', "
                f"created_at={self.created_at}, last_updated_at={self.last_updated_at})")
    
    def __eq__(self, other: object) -> bool:
        """Check equality based on symbol (case-insensitive)."""
        if not isinstance(other, Stock):
            return False
        return self.symbol.upper() == other.symbol.upper()
    
    def __hash__(self) -> int:
        """Hash based on symbol."""
        return hash(self.symbol.upper())