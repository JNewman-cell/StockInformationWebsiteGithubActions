"""
Utility functions for ticker overview table synchronization.
"""

import logging
import os
import sys
import time
from typing import Dict, List, Set, Tuple, Optional, Any
import yahooquery as yq  # type: ignore

# Add data layer to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer.models.ticker_overview import TickerOverview
from data_layer.repositories import TickerOverviewRepository

# Add entities and constants to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from entities.synchronization_result import SynchronizationResult
from constants import BATCH_SIZE, MAX_WORKERS

# Import common utilities
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from github_action_scripts.utils.utils import has_error, extract_error_message, convert_to_percentage, sanitize_decimal

logger = logging.getLogger(__name__)


def _fetch_yahoo_overview_data(
    tickers: List[str],
    session: Optional[Any] = None
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], List[str]]:
    """
    Fetch overview data from Yahoo Finance using key_stats and financial_data modules.

    Args:
        tickers: List of ticker symbols to lookup
        session: Optional user-managed session for API requests

    Returns:
        Tuple of:
        - Dictionary of key_stats data from Yahoo Finance
        - Dictionary of financial_data from Yahoo Finance
        - List of invalid symbols
    """
    ticker_kwargs: Dict[str, Any] = dict(
        verify=False,
        asynchronous=True,
        max_workers=MAX_WORKERS,
        validate=True,
    )

    if session is not None:
        stock = yq.Ticker(tickers, session=session, **ticker_kwargs)
    else:
        stock = yq.Ticker(tickers, **ticker_kwargs)
    
    # Get data from key_stats and financial_data modules in one API call
    modules_data = stock.get_modules(['defaultKeyStatistics', 'financialData'])  # type: ignore[assignment]
    
    # Reorganize data to match original format (ticker -> data)
    key_stats_data: Dict[str, Any] = {}
    financial_data: Dict[str, Any] = {}
    valuation_measures_data: Dict[str, Any] = {}
    
    for ticker in tickers:
        if ticker in modules_data:
            ticker_data = modules_data[ticker]  # type: ignore[assignment]
            key_stats_data[ticker] = ticker_data.get('defaultKeyStatistics', {})  # type: ignore[assignment]
            financial_data[ticker] = ticker_data.get('financialData', {})  # type: ignore[assignment]

    # Attempt to fetch current valuation measures using the financials APIs
    try:
        current_valuation = getattr(stock, 'current_valuation_measures', None)
        if callable(current_valuation):
            # Will return a dict keyed by symbol -> valuation measures record
            val_data = stock.current_valuation_measures()  # type: ignore[assignment]
            if isinstance(val_data, dict):
                valuation_measures_data = val_data
    except Exception:
        # Be defensive - if any exception occurs while fetching valuation measures,
        # continue without breaking existing behavior and leave valuation_measures_data empty.
        valuation_measures_data = {}
    
    # Get invalid symbols
    invalid_symbols: List[str] = []
    if hasattr(stock, 'invalid_symbols') and stock.invalid_symbols:
        invalid_symbols = stock.invalid_symbols
        logger.warning(f"Invalid symbols found: {invalid_symbols}")
    
    return key_stats_data, financial_data, valuation_measures_data, invalid_symbols  # type: ignore[return-value]


def get_ticker_overview_data_batch_from_yahoo_query(
    tickers: List[str],
    session: Optional[Any] = None
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """
    Lookup ticker overview data for multiple tickers using Yahoo Finance API.
    
    Fetches data from key_stats and financial_data modules and extracts:
    - Enterprise to EBITDA (enterpriseToEbitda)
    - Price to Book (priceToBook)
    - Gross Margin (grossMargins) - converted to percentage
    - Operating Margin (operatingMargins) - converted to percentage
    - Profit Margin (profitMargins) - converted to percentage
    - Earnings Growth (earningsGrowth) - converted to percentage
    - Revenue Growth (revenueGrowth) - converted to percentage
    - Trailing EPS (trailingEps)
    - Forward EPS (forwardEps)
    - PEG Ratio (pegRatio)
    
    Args:
        tickers: List of ticker symbols to lookup
        session: Optional user-managed session for API requests
        
    Returns:
        Tuple of:
        - Dictionary mapping ticker to overview data
        - List of tickers that failed lookup or had API errors
    """
    results: Dict[str, Dict[str, Any]] = {}
    failed_tickers: List[str] = []
    
    try:
        logger.info(f"Looking up ticker overview data for {len(tickers)} tickers...")

        key_stats_data: Dict[str, Any] = {}
        financial_data: Dict[str, Any] = {}
        invalid_symbols: List[str] = []

        # Single fetch - do not attempt retries for crumb failures
        key_stats_data, financial_data, valuation_measures_data, invalid_symbols = _fetch_yahoo_overview_data(tickers, session=session)

        # Check for invalid symbols
        if invalid_symbols:
            failed_tickers.extend(invalid_symbols)

        # Process each ticker
        for ticker in tickers:
            if ticker in failed_tickers:
                continue

            try:
                # Get data from both modules
                key_stats = key_stats_data.get(ticker) if key_stats_data else None
                fin_data = financial_data.get(ticker) if financial_data else None
                val_rec = valuation_measures_data.get(ticker) if valuation_measures_data else None

                
                # Check for errors in either dataset
                has_key_stats_error = key_stats and has_error(key_stats)
                has_fin_data_error = fin_data and has_error(fin_data)
                
                if has_key_stats_error or has_fin_data_error:
                    error_msg = (
                        extract_error_message(key_stats) if (has_key_stats_error and key_stats) 
                        else (extract_error_message(fin_data) if fin_data else "Unknown error")
                    )
                    logger.warning(f"Error fetching overview data for {ticker}: {error_msg}")
                    failed_tickers.append(ticker)
                    continue
                
                # If we have no data at all, skip
                if not key_stats and not fin_data:
                    logger.warning(f"No overview data available for ticker: {ticker}")
                    failed_tickers.append(ticker)
                    continue
                
                # Extract fields from valuation measures
                enterprise_to_ebitda = None
                price_to_book = None
                peg_ratio = None

                if val_rec and isinstance(val_rec, dict):
                    enterprise_to_ebitda = val_rec.get('EnterprisesValueEBITDARatio')
                    price_to_book = val_rec.get('PbRatio')
                    peg_ratio = val_rec.get('PegRatio')

                trailing_eps = None
                forward_eps = None
                # Extract fields from key_stats for values not found above
                if key_stats:
                    trailing_eps = key_stats.get('trailingEps')
                    forward_eps = key_stats.get('forwardEps')
                
                # Extract fields from financial_data and convert margins/growth to percentages
                gross_margin = None
                operating_margin = None
                profit_margin = None
                earnings_growth = None
                revenue_growth = None
                
                if fin_data:
                    # These are in 0.XXXX format, convert to XX.XX
                    gross_margin = convert_to_percentage(fin_data.get('grossMargins'))
                    operating_margin = convert_to_percentage(fin_data.get('operatingMargins'))
                    profit_margin = convert_to_percentage(fin_data.get('profitMargins'))
                    earnings_growth = convert_to_percentage(fin_data.get('earningsGrowth'))
                    revenue_growth = convert_to_percentage(fin_data.get('revenueGrowth'))
                
                # Sanitize all values to fit database constraints
                enterprise_to_ebitda = sanitize_decimal(enterprise_to_ebitda, 7, 2)
                price_to_book = sanitize_decimal(price_to_book, 7, 2)
                gross_margin = sanitize_decimal(gross_margin, 5, 2)
                operating_margin = sanitize_decimal(operating_margin, 5, 2)
                profit_margin = sanitize_decimal(profit_margin, 5, 2)
                earnings_growth = sanitize_decimal(earnings_growth, 9, 2)
                revenue_growth = sanitize_decimal(revenue_growth, 10, 2)
                trailing_eps = sanitize_decimal(trailing_eps, 7, 2)
                forward_eps = sanitize_decimal(forward_eps, 7, 2)
                peg_ratio = sanitize_decimal(peg_ratio, 7, 2)
                
                # Store the ticker data (all fields are optional)
                results[ticker] = {
                    'ticker': ticker,
                    'enterprise_to_ebitda': enterprise_to_ebitda,
                    'price_to_book': price_to_book,
                    'gross_margin': gross_margin,
                    'operating_margin': operating_margin,
                    'profit_margin': profit_margin,
                    'earnings_growth': earnings_growth,
                    'revenue_growth': revenue_growth,
                    'trailing_eps': trailing_eps,
                    'forward_eps': forward_eps,
                    'peg_ratio': peg_ratio
                }

                logger.debug(f"Successfully looked up overview for ticker: {ticker}")

            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {e}")
                failed_tickers.append(ticker)

        logger.info(f"Successfully looked up {len(results)} ticker overviews, {len(failed_tickers)} failed")

    except Exception as e:
        logger.error(f"Error during batch ticker overview lookup: {e}")
        raise RuntimeError(f"Failed to lookup ticker overview data: {e}")
    
    return results, failed_tickers


def process_tickers_and_persist_overviews(
    tickers: List[str],
    ticker_overview_repo: TickerOverviewRepository,
    database_ticker_overviews: Dict[str, TickerOverview],
    session: Optional[Any] = None,
) -> SynchronizationResult:
    """
    Process tickers in batches, lookup overview data from Yahoo Finance, and immediately persist to database.
    This ensures data is saved incrementally as it's retrieved, not all at once.
    
    Args:
        tickers: List of ticker symbols to process (from ticker_summary table)
        ticker_overview_repo: TickerOverview repository for database operations
        database_ticker_overviews: Dictionary of existing ticker overviews in database for comparison
        session: Optional user-managed session for Yahoo Finance API requests
        
    Returns:
        SynchronizationResult containing operation statistics
    """
    sync_result = SynchronizationResult()
    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"Processing {len(tickers)} tickers in {total_batches} batches of {BATCH_SIZE}")
    
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)...")
        logger.info(f"Waiting between batches to avoid rate limiting...")
        time.sleep(4)
        
        # Lookup ticker overview data
        batch_results, yahoo_failed = get_ticker_overview_data_batch_from_yahoo_query(batch, session=session)
        
        # Tickers that failed Yahoo lookup should be removed if they exist
        for failed_ticker in yahoo_failed:
            if failed_ticker in database_ticker_overviews:
                logger.info(f"Ticker {failed_ticker} failed Yahoo lookup and will be removed from database")
                sync_result.to_remove_due_to_errors.append(failed_ticker)
            sync_result.failed_ticker_lookups.append(failed_ticker)
        
        # Categorize ticker overviews and persist immediately
        overviews_to_add: List[TickerOverview] = []
        overviews_to_update: List[TickerOverview] = []
        
        for ticker, data in batch_results.items():
            try:
                # Create TickerOverview using from_dict
                new_overview = TickerOverview.from_dict(data)
                
                if ticker in database_ticker_overviews:
                    # Ticker exists - check if data changed
                    existing = database_ticker_overviews[ticker]
                    
                    # Compare all fields to see if update is needed
                    needs_update = (
                        existing.enterprise_to_ebitda != new_overview.enterprise_to_ebitda or
                        existing.price_to_book != new_overview.price_to_book or
                        existing.gross_margin != new_overview.gross_margin or
                        existing.operating_margin != new_overview.operating_margin or
                        existing.profit_margin != new_overview.profit_margin or
                        existing.earnings_growth != new_overview.earnings_growth or
                        existing.revenue_growth != new_overview.revenue_growth or
                        existing.trailing_eps != new_overview.trailing_eps or
                        existing.forward_eps != new_overview.forward_eps or
                        existing.peg_ratio != new_overview.peg_ratio
                    )
                    
                    if needs_update:
                        overviews_to_update.append(new_overview)
                    else:
                        # Unchanged - track it
                        sync_result.unchanged.append(ticker)
                else:
                    # New ticker - add it
                    overviews_to_add.append(new_overview)
                    
            except Exception as e:
                logger.error(f"Error creating TickerOverview for {ticker}: {e}")
                sync_result.failed_ticker_lookups.append(ticker)
        
        # Immediately persist new ticker overviews to database
        if overviews_to_add:
            try:
                added_count = ticker_overview_repo.bulk_insert(overviews_to_add)
                logger.info(f"Batch {batch_num}: Added {added_count} new ticker overviews to database")
                sync_result.to_add.extend(overviews_to_add)
                # Update local cache
                for overview in overviews_to_add:
                    database_ticker_overviews[overview.ticker] = overview
            except Exception as e:
                logger.error(f"Batch {batch_num}: Failed to add ticker overviews: {e}")
                raise
        
        # Immediately persist updated ticker overviews to database
        if overviews_to_update:
            try:
                updated_count = ticker_overview_repo.bulk_update(overviews_to_update)
                logger.info(f"Batch {batch_num}: Updated {updated_count} ticker overviews in database")
                sync_result.to_update.extend(overviews_to_update)
                # Update local cache
                for overview in overviews_to_update:
                    database_ticker_overviews[overview.ticker] = overview
            except Exception as e:
                logger.error(f"Batch {batch_num}: Failed to update ticker overviews: {e}")
                raise
    
    logger.info(f"Completed processing all {total_batches} batches")
    logger.info(f"Total: {len(sync_result.to_add)} added, {len(sync_result.to_update)} updated, "
                f"{len(sync_result.unchanged)} unchanged, {len(sync_result.failed_ticker_lookups)} failed lookups")
    
    return sync_result


def identify_tickers_to_delete(
    database_ticker_overviews: Dict[str, TickerOverview],
    processed_tickers: Set[str]
) -> List[str]:
    """
    Identify ticker overviews in database that were not found in the ticker_summary table.
    These should be deleted as they are no longer valid.
    
    Args:
        database_ticker_overviews: Dictionary of all ticker overviews currently in database
        processed_tickers: Set of ticker symbols that exist in ticker_summary table
        
    Returns:
        List of ticker symbols to delete from database
    """
    tickers_to_delete: List[str] = []
    
    for ticker in database_ticker_overviews.keys():
        if ticker not in processed_tickers:
            tickers_to_delete.append(ticker)
    
    if tickers_to_delete:
        logger.info(f"Found {len(tickers_to_delete)} ticker overviews in database that are not in ticker_summary")
    
    return tickers_to_delete


def delete_obsolete_ticker_overviews(
    ticker_overview_repo: TickerOverviewRepository,
    tickers_to_delete: List[str]
) -> int:
    """
    Delete ticker overviews from database that are no longer in ticker_summary table.
    
    Args:
        ticker_overview_repo: TickerOverview repository for database operations
        tickers_to_delete: List of ticker symbols to delete
        
    Returns:
        Number of ticker overviews successfully deleted
    """
    if not tickers_to_delete:
        logger.info("No obsolete ticker overviews to delete")
        return 0
    
    logger.info(f"Deleting {len(tickers_to_delete)} obsolete ticker overviews in batches of {BATCH_SIZE}")
    
    total_deleted = 0
    total_batches = (len(tickers_to_delete) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(tickers_to_delete), BATCH_SIZE):
        batch = tickers_to_delete[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        try:
            deleted_count = ticker_overview_repo.bulk_delete(batch)
            total_deleted += deleted_count
            logger.info(f"Delete batch {batch_num}/{total_batches}: Deleted {deleted_count}/{len(batch)} ticker overviews")
        except Exception as e:
            logger.error(f"Delete batch {batch_num}: Failed to delete ticker overviews: {e}")
            raise
    
    logger.info(f"Successfully deleted {total_deleted} obsolete ticker overviews from database")
    return total_deleted
