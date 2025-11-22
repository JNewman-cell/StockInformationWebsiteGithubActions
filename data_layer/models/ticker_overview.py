"""
Ticker overview model representing stock overview information.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from decimal import Decimal
import logging

from ..exceptions import ValidationError

logger = logging.getLogger(__name__)


@dataclass
class TickerOverview:
    """
    Represents a ticker overview entity with validation.
    
    Attributes:
        ticker: Stock ticker symbol (primary key, foreign key to ticker_summary)
        enterprise_to_ebitda: Enterprise value to EBITDA ratio
        price_to_book: Price to book ratio
        gross_margin: Gross profit margin (percentage, XX.XX format)
        operating_margin: Operating profit margin (percentage, XX.XX format)
        profit_margin: Net profit margin (percentage, XX.XX format)
        earnings_growth: Earnings growth rate (percentage, XX.XX format)
        revenue_growth: Revenue growth rate (percentage, XX.XX format)
        trailing_eps: Trailing 12-month earnings per share
        forward_eps: Forward earnings per share
        peg_ratio: Price/Earnings to Growth ratio
    """
    ticker: str
    enterprise_to_ebitda: Optional[Decimal] = None
    price_to_book: Optional[Decimal] = None
    gross_margin: Optional[Decimal] = None
    operating_margin: Optional[Decimal] = None
    profit_margin: Optional[Decimal] = None
    earnings_growth: Optional[Decimal] = None
    revenue_growth: Optional[Decimal] = None
    trailing_eps: Optional[Decimal] = None
    forward_eps: Optional[Decimal] = None
    peg_ratio: Optional[Decimal] = None
    ebitda_margin: Optional[Decimal] = None
    
    def __post_init__(self):
        """Clean and validate the ticker overview data after initialization."""
        # Clean ticker
        self.ticker = self.ticker.strip().upper()
        self.validate()
    
    def validate(self):
        """
        Validate ticker overview data.
        
        Raises:
            ValidationError: If validation fails
        """
        # Validate ticker
        if not self.ticker:
            raise ValidationError("ticker", self.ticker, "Ticker cannot be empty")
        
        if len(self.ticker) > 7:
            raise ValidationError("ticker", self.ticker, "Ticker cannot be longer than 7 characters")
        
        # Validate margins are in valid percentage range if provided
        # Margins can be negative (e.g., unprofitable companies)
        margin_fields = [
            ('gross_margin', self.gross_margin, Decimal('999.99')),
            ('operating_margin', self.operating_margin, Decimal('999.99')),
            ('profit_margin', self.profit_margin, Decimal('999.99'))
        ]
        
        for field_name, value, max_val in margin_fields:
            if value is not None:
                if value < Decimal('-999.99') or value > max_val:
                    raise ValidationError(field_name, value, f"{field_name} must be between -999.99 and 999.99")

        # Include ebitda_margin in margin validations (same range as other margins)
        if self.ebitda_margin is not None:
            if self.ebitda_margin < Decimal('-999.99') or self.ebitda_margin > Decimal('999.99'):
                raise ValidationError('ebitda_margin', self.ebitda_margin, 'ebitda_margin must be between -999.99 and 999.99')
        
        # Validate growth rates if provided
        # Growth rates can be negative (declining companies)
        growth_fields = [
            ('earnings_growth', self.earnings_growth, Decimal('9999999.99')),
            ('revenue_growth', self.revenue_growth, Decimal('99999999.99'))
        ]
        
        for field_name, value, max_val in growth_fields:
            if value is not None:
                if value < Decimal('-9999999.99') or value > max_val:
                    raise ValidationError(field_name, value, f"{field_name} must be within valid range")
        
        # Validate EPS values if provided (can be negative)
        eps_fields = [
            ('trailing_eps', self.trailing_eps),
            ('forward_eps', self.forward_eps)
        ]
        
        for field_name, value in eps_fields:
            if value is not None:
                if value < Decimal('-99999.99') or value > Decimal('99999.99'):
                    raise ValidationError(field_name, value, f"{field_name} must be within valid range")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert ticker overview to dictionary.
        
        Returns:
            Dictionary representation of the ticker overview
        """
        return {
            'ticker': self.ticker,
            'enterprise_to_ebitda': self.enterprise_to_ebitda,
            'price_to_book': self.price_to_book,
            'gross_margin': self.gross_margin,
            'operating_margin': self.operating_margin,
            'profit_margin': self.profit_margin,
            'earnings_growth': self.earnings_growth,
            'revenue_growth': self.revenue_growth,
            'trailing_eps': self.trailing_eps,
            'forward_eps': self.forward_eps,
            'peg_ratio': self.peg_ratio
            , 'ebitda_margin': self.ebitda_margin
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TickerOverview':
        """
        Create TickerOverview from dictionary.
        
        Args:
            data: Dictionary containing ticker overview data
            
        Returns:
            TickerOverview instance
        """
        return cls(
            ticker=data['ticker'],
            enterprise_to_ebitda=data.get('enterprise_to_ebitda'),
            price_to_book=data.get('price_to_book'),
            gross_margin=data.get('gross_margin'),
            operating_margin=data.get('operating_margin'),
            profit_margin=data.get('profit_margin'),
            earnings_growth=data.get('earnings_growth'),
            revenue_growth=data.get('revenue_growth'),
            trailing_eps=data.get('trailing_eps'),
            forward_eps=data.get('forward_eps'),
            peg_ratio=data.get('peg_ratio')
            , ebitda_margin=data.get('ebitda_margin')
        )
    
    def __eq__(self, other: object) -> bool:
        """
        Check equality with another TickerOverview.
        
        Args:
            other: Another TickerOverview instance
            
        Returns:
            True if all fields are equal, False otherwise
        """
        if not isinstance(other, TickerOverview):
            return False
        
        return (
            self.ticker == other.ticker and
            self.enterprise_to_ebitda == other.enterprise_to_ebitda and
            self.price_to_book == other.price_to_book and
            self.gross_margin == other.gross_margin and
            self.operating_margin == other.operating_margin and
            self.profit_margin == other.profit_margin and
            self.earnings_growth == other.earnings_growth and
            self.revenue_growth == other.revenue_growth and
            self.trailing_eps == other.trailing_eps and
            self.forward_eps == other.forward_eps and
            self.peg_ratio == other.peg_ratio
            and self.ebitda_margin == other.ebitda_margin
        )
    
    def __repr__(self) -> str:
        """String representation of TickerOverview."""
        return (
            f"TickerOverview(ticker={self.ticker}, "
            f"enterprise_to_ebitda={self.enterprise_to_ebitda}, "
            f"price_to_book={self.price_to_book}, "
            f"gross_margin={self.gross_margin}, "
            f"operating_margin={self.operating_margin}, "
            f"profit_margin={self.profit_margin}, ebitda_margin={self.ebitda_margin})"
        )
