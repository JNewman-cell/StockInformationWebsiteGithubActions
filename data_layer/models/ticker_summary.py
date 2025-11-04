"""
Ticker summary model representing stock summary information.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from decimal import Decimal, InvalidOperation
import logging

from ..exceptions import ValidationError

logger = logging.getLogger(__name__)


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
        
        # Validate dividend_yield if provided (NUMERIC(4,2) allows 0..99.99)
        if self.dividend_yield is not None and (self.dividend_yield < 0 or self.dividend_yield > Decimal('99.99')):
            raise ValidationError("dividend_yield", self.dividend_yield, "Dividend yield must be between 0 and 99.99")
        
        # Validate payout_ratio if provided (NUMERIC(4,2) allows 0..99.99)
        if self.payout_ratio is not None and (self.payout_ratio < 0 or self.payout_ratio > Decimal('99.99')):
            raise ValidationError("payout_ratio", self.payout_ratio, "Payout ratio must be between 0 and 99.99")
        
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
        # Helper to convert to Decimal safely and enforce finite values
        def to_decimal(value: Any, field_name: str, required: bool = False,
                       allow_negative: bool = False,
                       clamp_ratio_to_null: bool = False) -> Optional[Decimal]:
            if value is None:
                return None

            # Accept already-Decimal values
            try:
                dec = value if isinstance(value, Decimal) else Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError) as e:
                logger.warning(f"Invalid numeric value for {field_name}: {value} ({e})")
                if required:
                    raise ValidationError(field_name, value, f"Invalid numeric value: {e}")
                return None

            # Reject non-finite values (Infinity, NaN)
            try:
                if not dec.is_finite():
                    logger.warning(f"Non-finite value for {field_name}: {value}")
                    if required:
                        raise ValidationError(field_name, value, "Value must be finite")
                    return None
            except Exception:
                # Decimal may not have is_finite in some edge cases; be conservative
                logger.warning(f"Could not determine finiteness for {field_name}: {value}")

            # Enforce non-negative when requested
            if not allow_negative and dec < 0:
                logger.warning(f"Negative value for {field_name} coerced to None: {dec}")
                if required:
                    raise ValidationError(field_name, dec, f"{field_name} cannot be negative")
                return None

            # For ratio fields (dividend_yield, payout_ratio) clamp out-of-range to None
            # Schema uses NUMERIC(4,2) which supports up to 99.99, so allow 0..99.99
            if clamp_ratio_to_null:
                max_ratio = Decimal('99.99')
                if dec < 0 or dec > max_ratio:
                    logger.warning(f"{field_name} out of expected range 0..{max_ratio}; setting to None: {dec}")
                    return None

            return dec

        # Helper to convert market_cap to int safely
        def to_int(value: Any, field_name: str, required: bool = False) -> int:
            if value is None:
                if required:
                    raise ValidationError(field_name, value, f"{field_name} is required")
                return 0
            try:
                # market_cap may be float/str; convert via Decimal to avoid float precision
                dec = Decimal(str(value)) if not isinstance(value, int) else Decimal(value)
                if not dec.is_finite():
                    raise ValidationError(field_name, value, "Value must be finite")
                integer = int(dec)
            except (InvalidOperation, ValueError, TypeError) as e:
                raise ValidationError(field_name, value, f"Invalid integer value: {e}")
            if integer < 0:
                raise ValidationError(field_name, integer, f"{field_name} cannot be negative")
            return integer

        # Build sanitized fields
        market_cap = to_int(data.get('market_cap'), 'market_cap', required=True)
        previous_close = to_decimal(data.get('previous_close'), 'previous_close', required=True)
        fifty_day_average = to_decimal(data.get('fifty_day_average'), 'fifty_day_average', required=True)
        two_hundred_day_average = to_decimal(data.get('two_hundred_day_average'), 'two_hundred_day_average', required=True)

        # Static-type friendly checks: required fields must be present and finite
        if previous_close is None:
            raise ValidationError('previous_close', data.get('previous_close'), 'previous_close is required and must be finite')
        if fifty_day_average is None:
            raise ValidationError('fifty_day_average', data.get('fifty_day_average'), 'fifty_day_average is required and must be finite')
        if two_hundred_day_average is None:
            raise ValidationError('two_hundred_day_average', data.get('two_hundred_day_average'), 'two_hundred_day_average is required and must be finite')

        pe_ratio = to_decimal(data.get('pe_ratio'), 'pe_ratio', required=False, allow_negative=False)
        forward_pe_ratio = to_decimal(data.get('forward_pe_ratio'), 'forward_pe_ratio', required=False, allow_negative=False)
        # dividend_yield and payout_ratio are stored with precision/scale constrained in DB
        dividend_yield = to_decimal(data.get('dividend_yield'), 'dividend_yield', required=False, clamp_ratio_to_null=True)
        payout_ratio = to_decimal(data.get('payout_ratio'), 'payout_ratio', required=False, clamp_ratio_to_null=True)

        return cls(
            ticker=data['ticker'],
            cik=data.get('cik'),
            market_cap=market_cap,
            previous_close=previous_close,
            pe_ratio=pe_ratio,
            forward_pe_ratio=forward_pe_ratio,
            dividend_yield=dividend_yield,
            payout_ratio=payout_ratio,
            fifty_day_average=fifty_day_average,
            two_hundred_day_average=two_hundred_day_average
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
