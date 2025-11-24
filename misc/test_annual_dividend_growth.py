#!/usr/bin/env python3
"""
Simple script to compute annual dividend growth for AAPL using the same
data collection (yahooquery) and calculation logic used during ticker summary
sync, without calling the internal sync functions. This script is intended for
manual verification and demonstration (not a pytest).

It mirrors the following steps from the sync process:
 - Fetch summary_detail for the ticker from Yahoo via yahooquery
 - Convert dividend yields from fraction to percentage (0.008 -> 0.8)
 - Sanitize the decimals to DB constraints
 - Compute annual dividend growth: ((current - trailing)/trailing) * 100
 - Sanitize the resulting growth and treat exactly 0 as None
"""

from decimal import Decimal
from typing import Any, Optional
import logging
import json

import yahooquery as yq  # type: ignore

# The helpers below replicate the exact behaviour used by the sync utilities
# so we don't depend on importing internal modules and can run this file
# independently (useful for adhoc checks).
from decimal import InvalidOperation


def convert_to_percentage(value: Any) -> Optional[Decimal]:
    """Convert a fractional decimal (0.008) to percentage (0.8).

    Mirrors the behavior in `github_action_scripts/utils/utils.py`.
    Returns None if input is None or cannot be interpreted as a Decimal.
    """
    if value is None:
        return None
    try:
        decimal_val = Decimal(str(value))
        percentage = decimal_val * Decimal('100')
        return round(percentage, 2)
    except (InvalidOperation, ValueError, TypeError):
        return None


def sanitize_decimal(value: Any, max_digits: int = 7, decimal_places: int = 2) -> Optional[Decimal]:
    """Sanitize a numeric value similar to DB constraints used in the sync code.

    Returns a rounded Decimal if it fits the max digits / decimal places, or None
    if value is invalid, infinite, NaN, or out of range.
    """
    if value is None:
        return None
    try:
        decimal_val = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    try:
        if not decimal_val.is_finite():
            return None
    except Exception:
        return None

    rounded = round(decimal_val, decimal_places)
    max_value = Decimal(10 ** (max_digits - decimal_places)) - Decimal(10 ** (-decimal_places))
    min_value = -max_value
    if rounded < min_value or rounded > max_value:
        return None
    return rounded

logger = logging.getLogger(__name__)


def compute_annual_dividend_growth_from_raw(
    dividend_rate_raw: Any,
    trailing_rate_raw: Any,
    dividend_yield_raw: Any,
    trailing_yield_raw: Any,
) -> Optional[Decimal]:
    """Compute annual dividend growth using the same steps as the sync.

    Accepts raw values in the same format Yahoo provides (fractions like 0.008).
    Returns a sanitized Decimal or None when not computable.
    """
    # Rates (currency units) prefered; also build high-precision percentages for fallback
    raw_rate_current = Decimal(str(dividend_rate_raw)) if dividend_rate_raw is not None else None
    raw_rate_trailing = Decimal(str(trailing_rate_raw)) if trailing_rate_raw is not None else None
    raw_dividend_pct = Decimal(str(dividend_yield_raw)) * Decimal('100') if dividend_yield_raw is not None else None
    raw_trailing_pct = Decimal(str(trailing_yield_raw)) * Decimal('100') if trailing_yield_raw is not None else None

    # Sanitize yields to DB constraints (NUMERIC(5,2) behavior) for storage
    _ = sanitize_decimal(raw_dividend_pct, 5, 2)
    _ = sanitize_decimal(raw_trailing_pct, 5, 2)
    # Sanitize rates for storage
    _ = sanitize_decimal(raw_rate_current, 15, 2)
    _ = sanitize_decimal(raw_rate_trailing, 15, 2)

    if (raw_rate_current is None or raw_rate_trailing is None or raw_rate_trailing == Decimal('0')) and \
       (raw_dividend_pct is None or raw_trailing_pct is None or raw_trailing_pct == Decimal('0')):
        return None

    # Prefer rate-based growth, otherwise fallback to yields
    if raw_rate_current is not None and raw_rate_trailing is not None and raw_rate_trailing != Decimal('0'):
        raw_growth = (raw_rate_current - raw_rate_trailing) / raw_rate_trailing * Decimal('100')
    elif raw_dividend_pct is not None and raw_trailing_pct is not None and raw_trailing_pct != Decimal('0'):
        raw_growth = (raw_dividend_pct - raw_trailing_pct) / raw_trailing_pct * Decimal('100')
    else:
        return None
    annual_dividend_growth = sanitize_decimal(raw_growth, 5, 2)

    # If exactly 0 -> treat as None
    if annual_dividend_growth is not None and annual_dividend_growth == Decimal('0'):
        return None

    return annual_dividend_growth


def show_aapl_dividend_growth(ticker: str = 'INTU') -> None:
    print(f"Fetching Yahoo summary_detail for {ticker} (using yahooquery)...")

    stock = yq.Ticker(ticker, verify=False, asynchronous=False)
    summary = stock.summary_detail

    if not isinstance(summary, dict) or ticker.upper() not in summary:
        logger.error('No summary_detail data for ticker %s', ticker)
        print('No data available for ticker:', ticker)
        return

    data = summary[ticker.upper()]

    # Dump a small subset of fields for context
    print(json.dumps({
        'ticker': ticker.upper(),
        'dividendYield': data.get('dividendYield'),
        'trailingAnnualDividendYield': data.get('trailingAnnualDividendYield'),
        'dividendRate': data.get('dividendRate'),
        'trailingAnnualDividendRate': data.get('trailingAnnualDividendRate')
    }, indent=2, default=str))

    # Compute using the same exact process the sync uses (prefer rates, fallback to yields)
    growth = compute_annual_dividend_growth_from_raw(
        data.get('dividendRate'),
        data.get('trailingAnnualDividendRate'),
        data.get('dividendYield'),
        data.get('trailingAnnualDividendYield'),
    )

    print('\nComputed annual_dividend_growth (using sync logic steps):')
    print('  annual_dividend_growth:', growth)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    show_aapl_dividend_growth('INTUrem')

