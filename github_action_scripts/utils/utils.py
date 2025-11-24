"""
Common utility functions shared across GitHub action scripts.
"""

import logging
import requests
from typing import Dict, List, Tuple, Any, cast, Optional
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)


# ============================================================================
# Common Stock Filtering
# ============================================================================

# Keywords that indicate the security is NOT common stock
NON_COMMON_STOCK_KEYWORDS = [
    # Debt instruments
    "Junior Subordinated",
    "Subordinated Notes",
    "Subordinated Debentures",
    "Senior Notes",
    "Mortgage Bonds",
    "Debentures",
    "Notes due",
    "Notes Due",
    "Notes Exp",
    "Global Notes",
    "ZONES",
    
    # Preferred stock indicators
    "Preferred",
    "Preferred Stock",
    "Preferred Shares",
    "Pref",
    "Pref Shs",
    "Perpetual",
    "Cumulative Redeemable",
    "Non-Cumulative",
    "Non Cumulative",
    "Redeemable Preferred",
    "Preference Shares",
    "Mandatory Convertible Preferred",
    
    # Rate types (typically for preferred stocks or debt)
    "Fixed to Floating",
    "Fixed-to-Floating",
    "Fixed-Rate",
    "Fixed Rate",
    "Floating Rate",
    
    # Series indicators (usually preferred stocks)
    "Series A",
    "Series B",
    "Series C",
    "Series D",
    "Series E",
    "Series F",
    "Series G",
    "Series H",
    "Series I",
    "Series J",
    "Series K",
    "Series L",
    "Series M",
    "Series N",
    "Series O",
    "Series P",
    "Series Q",
    "Series R",
    "Series S",
    "Series T",
    "Series 202",  # For series with years
    
    # Depositary shares (representing fractional interests)
    "Depositary Shares",
    "Depositary Share",
    "DS Rep",
    "DS Representing",
    "1000 DS",
    "Representing a 1/1000th",
    "Representing 1/1000th",
    "representing a 1/20th",
    "Liquidation Preference",
    
    # Warrants and Rights
    "Warrant",
    "Warrants",
    "Right",
    "Rights",
    "Stakeholder Warrants",
    
    # Units (typically SPAC units or combinations of securities)
    "Units",
    "Unit",
    
    # American Depositary Shares and similar
    "American Depositary Shares",
    "American Depositary Share",
    "ADS Representing",  # ADS representing ordinary shares
    "Representing 1 Ord",  # Representing ordinary shares
    
    # Other exclusions
    "par value",  # Par value notation should be filtered
    "Par Value",
    
    # Convertible securities
    "Convertible",
    "Exchangeable",
    "Conv.",
    "Conv Pref",
    
    # Specific structures
    "Exp 20",  # Expiration dates
    "due 20",  # Due dates
    "due 2",   # Shorter pattern for due dates
    "Term Pref",
]


def is_common_stock(ticker_name: str) -> bool:
    """
    Determines if a ticker name represents common stock.
    
    Args:
        ticker_name: The name of the security
        
    Returns:
        True if the ticker is likely common stock, False otherwise
    """
    ticker_name_upper = ticker_name.upper()
    
    # HIGHEST PRIORITY: If it explicitly says "Common Stock", ALWAYS keep it
    # This overrides everything else
    if "COMMON STOCK" in ticker_name_upper:
        return True
    
    # For securities that don't explicitly say "Common Stock", apply filters:
    
    # Filter out American Depositary Shares (ADS)
    if any(keyword in ticker_name_upper for keyword in [
        "AMERICAN DEPOSITARY SHARES",
        "AMERICAN DEPOSITARY SHARE",
        "ADS REPRESENTING"
    ]):
        return False
    
    # Filter out Depositary Shares representing preferred/other securities
    if "DEPOSITARY SHARES" in ticker_name_upper and "REPRESENTING" in ticker_name_upper:
        return False
    
    # Check if any non-common stock keyword is present
    for keyword in NON_COMMON_STOCK_KEYWORDS:
        keyword_upper = keyword.upper()
        
        if keyword_upper in ticker_name_upper:
            return False
    
    # If no exclusion keywords found, consider it common stock
    return True


def fetch_ticker_data_from_github_repo() -> List[str]:
    """Fetch ticker data directly from the Improved-US-Stock-Symbols GitHub repository.
    
    Uses the 'all_tickers.json' file which contains symbols from all exchanges.
    Filters out non-common stocks using the is_common_stock function.
    
    Returns:
        List of normalized ticker symbols (common stocks only)
    """
    tickers: List[str] = []
    
    # URL for the all tickers JSON file - contains symbols from all US exchanges
    url = 'https://raw.githubusercontent.com/JNewman-cell/Improved-US-Stock-Symbols/main/all/all_full_tickers.json'
    
    try:
        logger.info("Fetching all US ticker data from GitHub repository...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse JSON response - should be a list of ticker objects
        raw = response.json()

        if not isinstance(raw, list):
            logger.error(f"Unexpected JSON format: expected list, got {type(raw)}")
            raise RuntimeError("Invalid JSON format received from GitHub repository")

        # Cast to List[Any] so we can still validate each element at runtime
        ticker_data = cast(List[Any], raw)

        # Process each ticker object
        caret_filtered_count = 0
        non_common_filtered_count = 0
        length_filtered_count = 0
        
        for ticker_obj in ticker_data:
            # Validate that each item is a dictionary with required fields
            if not isinstance(ticker_obj, dict):
                logger.warning(f"Skipping invalid ticker object: {ticker_obj}")
                continue

            # Narrow the type for static checkers
            ticker_map = cast(Dict[str, Any], ticker_obj)

            symbol: str = str(ticker_map.get('symbol', '')).strip()
            name: str = str(ticker_map.get('name', '')).strip()
            
            if not symbol:
                continue
            
            # Skip tickers with ^ character (preferred shares, warrants, etc.)
            if '^' in symbol:
                caret_filtered_count += 1
                continue
            
            # Filter out non-common stocks based on name
            if name and not is_common_stock(name):
                non_common_filtered_count += 1
                continue
                
            # Normalize ticker by replacing / and \ with - to follow Yahoo Finance conventions
            normalized_ticker = symbol.upper().replace('/', '-').replace('\\', '-')
            
            # Filter out tickers longer than 6 characters (likely invalid)
            if len(normalized_ticker) > 6:
                length_filtered_count += 1
                continue
            
            tickers.append(normalized_ticker)
        
        logger.info(f"Successfully loaded {len(tickers)} common stock ticker symbols from GitHub repository")
        if caret_filtered_count > 0:
            logger.info(f"Filtered out {caret_filtered_count} tickers containing '^' character (preferred shares, warrants, etc.)")
        if non_common_filtered_count > 0:
            logger.info(f"Filtered out {non_common_filtered_count} non-common stock securities")
        if length_filtered_count > 0:
            logger.info(f"Filtered out {length_filtered_count} tickers longer than 6 characters")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching ticker data from GitHub: {e}")
        raise RuntimeError(f"Failed to fetch ticker data from GitHub repository: {e}")
    except ValueError as e:
        logger.error(f"Error parsing JSON response: {e}")
        raise RuntimeError(f"Failed to parse ticker data from GitHub repository: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching ticker data: {e}")
        raise RuntimeError(f"Unexpected error fetching ticker data: {e}")
    
    if not tickers:
        raise RuntimeError("No valid ticker symbols found in GitHub repository")
    
    return tickers


def lookup_cik_batch(tickers: List[str]) -> Tuple[Dict[str, int], List[str]]:
    """
    Lookup CIK for multiple tickers using sec-company-lookup.
    
    Args:
        tickers: List of ticker symbols to lookup
        
    Returns:
        Tuple of:
        - Dictionary mapping ticker to CIK
        - List of tickers that failed CIK lookup
    """
    from sec_company_lookup import get_companies_by_tickers
    
    results: Dict[str, int] = {}
    failed_tickers: List[str] = []
    
    try:
        logger.info(f"Looking up CIK for {len(tickers)} tickers...")
        batch_results = get_companies_by_tickers(tickers)
        
        if batch_results is None:
            logger.error("CIK batch lookup returned None")
            raise RuntimeError("Failed to lookup CIKs: batch lookup returned None")
        
        for ticker in tickers:
            if ticker in batch_results:  # type: ignore
                result = batch_results[ticker]  # type: ignore
                
                if result.get('success') and result.get('data'):  # type: ignore
                    company_data = result['data']  # type: ignore
                    cik = company_data.get('cik')  # type: ignore
                    
                    if cik is not None:
                        results[ticker] = cik
                    else:
                        logger.debug(f"No CIK found for ticker {ticker}")
                        failed_tickers.append(ticker)
                else:
                    logger.debug(f"Failed to lookup CIK for ticker {ticker}: {result.get('error', 'Unknown error')}")  # type: ignore
                    failed_tickers.append(ticker)
            else:
                logger.debug(f"No CIK result for ticker {ticker}")
                failed_tickers.append(ticker)
        
        logger.info(f"Successfully looked up CIK for {len(results)} tickers, {len(failed_tickers)} failed")
        
    except Exception as e:
        logger.error(f"Error during batch CIK lookup: {e}")
        raise RuntimeError(f"Failed to lookup CIKs: {e}")
    
    return results, failed_tickers


# ============================================================================
# Yahoo Finance API Utilities
# ============================================================================

def has_error(item: Dict[str, Any]) -> bool:
    """Check if the response item contains an error.

    Checks for the current structured 'error' object returned by the Yahoo endpoint.
    Returns True if an error is found.
    
    Expected structure: {'AAPL': {'error': {'code': 404, 'type': 'NotFoundError', 
                                           'message': '...', 'symbol': 'AAPL'}}}
    """
    return bool(item.get('error'))


def extract_error_message(item: Dict[str, Any]) -> Optional[str]:
    """Return an error message string if the response item contains an error.

    Extracts error message from the current structured 'error' object
    returned by the Yahoo endpoint. Returns None when no error is found.
    
    Expected structure: {'AAPL': {'error': {'code': 404, 'type': 'NotFoundError', 
                                           'message': '...', 'symbol': 'AAPL'}}}
    """
    if error_obj := item.get('error'):
        return error_obj.get('message') or error_obj.get('type')
    
    return None


def convert_to_percentage(value: Any) -> Optional[Decimal]:
    """
    Convert a decimal value (0.XXXX format) to percentage (XX.XX format).
    For example, 0.1234 becomes 12.34
    
    Args:
        value: Value to convert (can be None, numeric, or string)
        
    Returns:
        Converted percentage value or None if invalid
    """
    if value is None:
        return None
    
    try:
        decimal_val = Decimal(str(value))
        # Only convert fractional values (absolute <= 1) to percentages so
        # we avoid double-scaling values that are already percentages.
        if decimal_val.copy_abs() <= Decimal('1'):
            percentage = decimal_val * Decimal('100')
        else:
            percentage = decimal_val
        # Round to 2 decimal places
        return round(percentage, 2)
    except (InvalidOperation, ValueError, TypeError):
        return None


def sanitize_decimal(value: Any, max_digits: int = 7, decimal_places: int = 2) -> Optional[Decimal]:
    """
    Sanitize a numeric value to fit within database constraints.
    
    Args:
        value: Value to sanitize
        max_digits: Maximum total digits (including decimal places)
        decimal_places: Number of decimal places
        
    Returns:
        Sanitized Decimal value or None if invalid
    """
    if value is None:
        return None
    
    try:
        decimal_val = Decimal(str(value))
        
        # Check for special values
        if decimal_val.is_nan() or decimal_val.is_infinite():
            return None
        
        # Round to specified decimal places
        rounded = round(decimal_val, decimal_places)
        
        # Check if value fits within the numeric type constraints
        max_value = Decimal(10 ** (max_digits - decimal_places)) - Decimal(10 ** (-decimal_places))
        min_value = -max_value
        
        if rounded < min_value or rounded > max_value:
            logger.warning(f"Value {rounded} exceeds database constraints, setting to None")
            return None
        
        return rounded
    except (InvalidOperation, ValueError, TypeError):
        return None
