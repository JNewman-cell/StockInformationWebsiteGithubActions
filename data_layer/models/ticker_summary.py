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
        
        # PE ratios may be negative (companies with negative earnings).
        # No validation to forbid negative pe_ratio/forward_pe_ratio here.
        # If you want to enforce a range, update this method accordingly.
        
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

            # Note: negative values are allowed for fields like PE ratios.
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

        # PE ratios may be negative; preserve negative values
        pe_ratio = to_decimal(data.get('pe_ratio'), 'pe_ratio', required=False)
        forward_pe_ratio = to_decimal(data.get('forward_pe_ratio'), 'forward_pe_ratio', required=False)

        # dividend_yield and payout_ratio are stored with precision/scale constrained in DB
        dividend_yield = to_decimal(data.get('dividend_yield'), 'dividend_yield', required=False, clamp_ratio_to_null=True)
        payout_ratio = to_decimal(data.get('payout_ratio'), 'payout_ratio', required=False, clamp_ratio_to_null=True)

        # Convert dividend_yield from fraction to percentage when appropriate.
        # Many data sources return dividend_yield as a fraction (e.g., 0.02 for 2%).
        # If the value is <= 1, assume it's a fraction and multiply by 100 so the DB stores a percent.
        if dividend_yield is not None:
            try:
                if dividend_yield <= Decimal('1'):
                    dividend_yield = dividend_yield * Decimal('100')
                    # Re-run clamp check to ensure within DB range after scaling
                    if dividend_yield < 0 or dividend_yield > Decimal('99.99'):
                        logger.warning(f"dividend_yield after scaling out of range; setting to None: {dividend_yield}")
                        dividend_yield = None
            except Exception:
                # If any comparison/conversion fails, set to None to be safe
                logger.warning(f"Could not scale dividend_yield value; setting to None")
                dividend_yield = None

        # Convert payout_ratio from fraction to percentage when appropriate.
        # Many data sources return payout_ratio as a fraction (e.g., 0.25 for 25%).
        # If the value is <= 1, assume it's a fraction and multiply by 100 so the DB stores a percent.
        if payout_ratio is not None:
            try:
                if payout_ratio <= Decimal('1'):
                    payout_ratio = payout_ratio * Decimal('100')
                    # Re-run clamp check to ensure within DB range after scaling
                    if payout_ratio < 0 or payout_ratio > Decimal('99.99'):
                        logger.warning(f"payout_ratio after scaling out of range; setting to None: {payout_ratio}")
                        payout_ratio = None
            except Exception:
                # If any comparison/conversion fails, set to None to be safe
                logger.warning(f"Could not scale payout_ratio value; setting to None")
                payout_ratio = None

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
