"""
Ticker summary model representing stock summary information.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from decimal import Decimal

from ..exceptions import ValidationError


@dataclass
class TickerSummary:
    """
    Represents a ticker summary entity with validation.
    
    Attributes:
        ticker: Stock ticker symbol (primary key)
        cik: Central Index Key (foreign key to cik_lookup)
        market_cap: Market capitalization
        previous_close: Previous closing price
        pe_ratio: Price-to-earnings ratio
        forward_pe_ratio: Forward price-to-earnings ratio
        dividend_yield: Dividend yield percentage
        payout_ratio: Dividend payout ratio
        fifty_day_average: 50-day moving average
        two_hundred_day_average: 200-day moving average
    """
    ticker: str
    market_cap: int
    previous_close: Decimal
    fifty_day_average: Decimal
    two_hundred_day_average: Decimal
    cik: Optional[int] = None
    pe_ratio: Optional[Decimal] = None
    forward_pe_ratio: Optional[Decimal] = None
    dividend_yield: Optional[Decimal] = None
    payout_ratio: Optional[Decimal] = None
    
    def __post_init__(self):
        """Clean and validate the ticker summary data after initialization."""
        # Clean ticker
        self.ticker = self.ticker.strip().upper()
        self.validate()
    
    def validate(self):
        """
        Validate ticker summary data.
        
        Raises:
            ValidationError: If validation fails
        """
        # Validate ticker
        if not self.ticker:
            raise ValidationError("ticker", self.ticker, "Ticker cannot be empty")
        
        if len(self.ticker) > 7:
            raise ValidationError("ticker", self.ticker, "Ticker cannot be longer than 7 characters")
        
        # Validate CIK if provided
        if self.cik is not None and self.cik <= 0:
            raise ValidationError("cik", self.cik, "CIK must be a positive integer")
        
        # Validate market_cap
        if self.market_cap < 0:
            raise ValidationError("market_cap", self.market_cap, "Market cap cannot be negative")
        
        # Validate previous_close
        if self.previous_close < 0:
            raise ValidationError("previous_close", self.previous_close, "Previous close cannot be negative")
        
        # Validate pe_ratio if provided
        if self.pe_ratio is not None and self.pe_ratio < 0:
            raise ValidationError("pe_ratio", self.pe_ratio, "PE ratio cannot be negative")
        
        # Validate forward_pe_ratio if provided
        if self.forward_pe_ratio is not None and self.forward_pe_ratio < 0:
            raise ValidationError("forward_pe_ratio", self.forward_pe_ratio, "Forward PE ratio cannot be negative")
        
        # Validate dividend_yield if provided
        if self.dividend_yield is not None and (self.dividend_yield < 0 or self.dividend_yield > 1):
            raise ValidationError("dividend_yield", self.dividend_yield, "Dividend yield must be between 0 and 1")
        
        # Validate payout_ratio if provided
        if self.payout_ratio is not None and (self.payout_ratio < 0 or self.payout_ratio > 1):
            raise ValidationError("payout_ratio", self.payout_ratio, "Payout ratio must be between 0 and 1")
        
        # Validate fifty_day_average
        if self.fifty_day_average < 0:
            raise ValidationError("fifty_day_average", self.fifty_day_average, "50-day average cannot be negative")
        
        # Validate two_hundred_day_average
        if self.two_hundred_day_average < 0:
            raise ValidationError("two_hundred_day_average", self.two_hundred_day_average, "200-day average cannot be negative")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert ticker summary to dictionary.
        
        Returns:
            Dictionary representation of the ticker summary
        """
        return {
            'ticker': self.ticker,
            'cik': self.cik,
            'market_cap': self.market_cap,
            'previous_close': float(self.previous_close) if self.previous_close else None,
            'pe_ratio': float(self.pe_ratio) if self.pe_ratio else None,
            'forward_pe_ratio': float(self.forward_pe_ratio) if self.forward_pe_ratio else None,
            'dividend_yield': float(self.dividend_yield) if self.dividend_yield else None,
            'payout_ratio': float(self.payout_ratio) if self.payout_ratio else None,
            'fifty_day_average': float(self.fifty_day_average) if self.fifty_day_average else None,
            'two_hundred_day_average': float(self.two_hundred_day_average) if self.two_hundred_day_average else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TickerSummary':
        """
        Create TickerSummary instance from dictionary.
        
        Args:
            data: Dictionary containing ticker summary data
        
        Returns:
            TickerSummary instance
        """
        # Convert numeric fields to appropriate types
        def to_decimal(value: Any) -> Optional[Decimal]:
            if value is None:
                return None
            return Decimal(str(value))
        
        return cls(
            ticker=data['ticker'],
            cik=data.get('cik'),
            market_cap=data['market_cap'],
            previous_close=Decimal(str(data['previous_close'])),
            pe_ratio=to_decimal(data.get('pe_ratio')),
            forward_pe_ratio=to_decimal(data.get('forward_pe_ratio')),
            dividend_yield=to_decimal(data.get('dividend_yield')),
            payout_ratio=to_decimal(data.get('payout_ratio')),
            fifty_day_average=Decimal(str(data['fifty_day_average'])),
            two_hundred_day_average=Decimal(str(data['two_hundred_day_average']))
        )
    
    def __repr__(self) -> str:
        """String representation of ticker summary."""
        return f"TickerSummary(ticker='{self.ticker}', market_cap={self.market_cap}, previous_close={self.previous_close})"
    
    def __eq__(self, other: object) -> bool:
        """Compare two TickerSummary instances."""
        if not isinstance(other, TickerSummary):
            return False
        return self.ticker == other.ticker
    
    def __hash__(self) -> int:
        """Hash based on ticker (primary key)."""
        return hash(self.ticker)
