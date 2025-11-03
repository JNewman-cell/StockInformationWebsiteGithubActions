"""
Utility functions for ticker summary table synchronization.
"""

import logging
import os
import sys
import time
import requests
from typing import Dict, List, Set, Tuple, Optional, Any
from decimal import Decimal
import yahooquery as yq  # type: ignore

# Add data layer to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer.models.ticker_summary import TickerSummary
from data_layer.repositories import TickerSummaryRepository

# Add entities and constants to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from entities.synchronization_result import SynchronizationResult
from constants import BATCH_SIZE, MAX_WORKERS

logger = logging.getLogger(__name__)


def fetch_ticker_data_from_github_repo() -> List[str]:
    """Fetch ticker data directly from the Improved-US-Stock-Symbols GitHub repository.
    
    Uses the 'all_tickers.json' file which contains symbols from all exchanges.
    
    Returns:
        List of normalized ticker symbols
    """
    tickers: List[str] = []
    
    # URL for the all tickers JSON file - contains symbols from all US exchanges
    url = 'https://raw.githubusercontent.com/JNewman-cell/Improved-US-Stock-Symbols/main/all/all_tickers.json'
    
    try:
        logger.info("Fetching all US ticker data from GitHub repository...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse JSON response - should be a simple array of ticker symbols
        ticker_symbols: List[str] = response.json()  # type: ignore
        
        if not isinstance(ticker_symbols, list):  # type: ignore
            logger.error(f"Unexpected JSON format: expected list, got {type(ticker_symbols)}")
            raise RuntimeError("Invalid JSON format received from GitHub repository")
            
        # Process each ticker symbol
        filtered_count = 0
        for ticker in ticker_symbols:
            if isinstance(ticker, str) and ticker.strip():  # type: ignore
                # Skip tickers with ^ character (preferred shares, warrants, etc.)
                if '^' in ticker:
                    filtered_count += 1
                    continue
                    
                # Normalize ticker by replacing / and \ with - to follow Yahoo Finance conventions
                normalized_ticker = ticker.strip().upper().replace('/', '-').replace('\\', '-')
                # Filter out tickers longer than 6 characters (likely invalid)
                if len(normalized_ticker) <= 6:
                    tickers.append(normalized_ticker)
        
        logger.info(f"Successfully loaded {len(tickers)} ticker symbols from GitHub repository")
        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} tickers containing '^' character (preferred shares, warrants, etc.)")
        
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


def _has_error(item: Dict[str, Any]) -> bool:
    """Check if the response item contains an error.

    Checks for the current structured 'error' object returned by the Yahoo endpoint.
    Returns True if an error is found.
    
    Expected structure: {'EAI': {'error': {'code': 404, 'type': 'NotFoundError', 
                                           'message': '...', 'symbol': 'EAI'}}}
    """
    return bool(item.get('error'))


def _extract_error_message(item: Dict[str, Any]) -> Optional[str]:
    """Return an error message string if the response item contains an error.

    Extracts error message from the current structured 'error' object
    returned by the Yahoo endpoint. Returns None when no error is found.
    
    Expected structure: {'EAI': {'error': {'code': 404, 'type': 'NotFoundError', 
                                           'message': '...', 'symbol': 'EAI'}}}
    """
    if error_obj := item.get('error'):
        return error_obj.get('message') or error_obj.get('type')
    
    return None


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


def get_ticker_summary_data_batch_from_yahoo_query(tickers: List[str]) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """
    Lookup ticker summary data for multiple tickers using Yahoo Finance API.
    
    Args:
        tickers: List of ticker symbols to lookup
        
    Returns:
        Tuple of:
        - Dictionary mapping ticker to summary data
        - List of tickers that failed lookup or had API errors
    """
    results: Dict[str, Dict[str, Any]] = {}
    failed_tickers: List[str] = []
    
    try:
        logger.info(f"Looking up ticker summary data for {len(tickers)} tickers...")
        
        # Create Ticker object with batch of symbols and asynchronous processing
        stock = yq.Ticker(
            tickers,
            asynchronous=True,
            max_workers=MAX_WORKERS,
            validate=True
        )
        
        # Get data from multiple endpoints
        summary_data: Dict[str, Any] = stock.summary_detail  # type: ignore
        
        # Check for invalid symbols
        if hasattr(stock, 'invalid_symbols') and stock.invalid_symbols:
            failed_tickers.extend(stock.invalid_symbols)
            logger.warning(f"Invalid symbols found: {stock.invalid_symbols}")
        
        # Process each ticker
        for ticker in tickers:
            if ticker in failed_tickers:
                continue
            
            try:
                # Check if we have summary data for this ticker
                if ticker not in summary_data or summary_data[ticker] is None:
                    logger.warning(f"No summary data available for ticker: {ticker}")
                    failed_tickers.append(ticker)
                    continue
                
                # Check if there's an error in the data
                if _has_error(summary_data[ticker]):  # type: ignore
                    error_msg = _extract_error_message(summary_data[ticker])  # type: ignore
                    logger.warning(f"Error fetching summary data from yahoo for {ticker}: {error_msg}")
                    failed_tickers.append(ticker)
                    continue
                
                symbol_info: Dict[str, Any] = summary_data[ticker]  # type: ignore
                
                # Extract required fields
                market_cap = symbol_info.get('marketCap')  # type: ignore
                if market_cap is None or market_cap == 0:
                    logger.warning(f"No market cap available for ticker: {ticker}")
                    failed_tickers.append(ticker)
                    continue
                
                previous_close = symbol_info.get('previousClose')  # type: ignore
                fifty_day_avg = symbol_info.get('fiftyDayAverage')  # type: ignore
                two_hundred_day_avg = symbol_info.get('twoHundredDayAverage')  # type: ignore
                
                # Validate required fields
                if previous_close is None or fifty_day_avg is None or two_hundred_day_avg is None:
                    logger.warning(f"Missing required fields for ticker: {ticker}")
                    failed_tickers.append(ticker)
                    continue
                
                # Extract optional fields
                pe_ratio = symbol_info.get('trailingPE')  # type: ignore
                forward_pe = symbol_info.get('forwardPE')  # type: ignore
                dividend_yield = symbol_info.get('dividendYield')  # type: ignore
                payout_ratio = symbol_info.get('payoutRatio')  # type: ignore
                
                # Store the ticker data
                results[ticker] = {
                    'ticker': ticker,
                    'market_cap': market_cap,
                    'previous_close': previous_close,
                    'pe_ratio': pe_ratio,
                    'forward_pe_ratio': forward_pe,
                    'dividend_yield': dividend_yield,
                    'payout_ratio': payout_ratio,
                    'fifty_day_average': fifty_day_avg,
                    'two_hundred_day_average': two_hundred_day_avg
                }
                
                logger.debug(f"Successfully looked up ticker: {ticker}")
                
            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {e}")
                failed_tickers.append(ticker)
        
        logger.info(f"Successfully looked up {len(results)} tickers, {len(failed_tickers)} failed")
        
    except Exception as e:
        logger.error(f"Error during batch ticker lookup: {e}")
        raise RuntimeError(f"Failed to lookup ticker summary data: {e}")
    
    return results, failed_tickers


def process_tickers_and_persist_summaries(
    tickers: List[str],
    ticker_summary_repo: TickerSummaryRepository,
    database_ticker_summaries: Dict[str, TickerSummary]
) -> SynchronizationResult:
    """
    Process tickers in batches, lookup summary data from Yahoo Finance, and immediately persist to database.
    This ensures data is saved incrementally as it's retrieved, not all at once.
    
    Args:
        tickers: List of ticker symbols to process
        ticker_summary_repo: TickerSummary repository for database operations
        database_ticker_summaries: Dictionary of existing ticker summaries in database for comparison
        
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
        
        # Step 1: Lookup CIK for this batch (validates companies are real)
        batch_ciks, cik_failed = lookup_cik_batch(batch)
        
        # Tickers that failed CIK lookup should be removed from database if they exist
        for failed_ticker in cik_failed:
            if failed_ticker in database_ticker_summaries:
                logger.info(f"Ticker {failed_ticker} failed CIK lookup and will be removed from database")
                sync_result.to_remove_due_to_errors.append(failed_ticker)
            else:
                logger.debug(f"Ticker {failed_ticker} failed CIK lookup, skipping")
            sync_result.failed_ticker_lookups.append(failed_ticker)
        
        # Only process tickers that have valid CIKs
        tickers_with_cik = list(batch_ciks.keys())
        if not tickers_with_cik:
            logger.warning(f"Batch {batch_num}: No tickers with valid CIK, skipping Yahoo lookup")
            continue
        
        # Step 2: Lookup ticker summary data for tickers with valid CIKs
        batch_results, yahoo_failed = get_ticker_summary_data_batch_from_yahoo_query(tickers_with_cik)
        
        # Tickers that failed Yahoo lookup should also be removed if they exist
        for failed_ticker in yahoo_failed:
            if failed_ticker in database_ticker_summaries:
                logger.info(f"Ticker {failed_ticker} failed Yahoo lookup and will be removed from database")
                sync_result.to_remove_due_to_errors.append(failed_ticker)
            sync_result.failed_ticker_lookups.append(failed_ticker)
        
        # Step 3: Categorize ticker summaries and persist immediately
        summaries_to_add: List[TickerSummary] = []
        summaries_to_update: List[TickerSummary] = []
        
        for ticker, data in batch_results.items():
            try:
                # Validate required fields are not empty/zero
                market_cap = data['market_cap']
                previous_close = data['previous_close']
                
                if market_cap is None or market_cap <= 0:
                    logger.warning(f"Ticker {ticker} has invalid market_cap ({market_cap}), skipping")
                    if ticker in database_ticker_summaries:
                        sync_result.to_remove_due_to_errors.append(ticker)
                    sync_result.failed_ticker_lookups.append(ticker)
                    continue
                
                if previous_close is None or previous_close <= 0:
                    logger.warning(f"Ticker {ticker} has invalid previous_close ({previous_close}), skipping")
                    if ticker in database_ticker_summaries:
                        sync_result.to_remove_due_to_errors.append(ticker)
                    sync_result.failed_ticker_lookups.append(ticker)
                    continue
                
                # Get CIK for this ticker (we know it exists from batch_ciks)
                cik = batch_ciks.get(ticker)
                
                # Create TickerSummary object with CIK from SEC lookup
                new_summary = TickerSummary(
                    ticker=ticker,
                    cik=cik,
                    market_cap=market_cap,
                    previous_close=Decimal(str(previous_close)),
                    pe_ratio=Decimal(str(data['pe_ratio'])) if data['pe_ratio'] is not None else None,
                    forward_pe_ratio=Decimal(str(data['forward_pe_ratio'])) if data['forward_pe_ratio'] is not None else None,
                    dividend_yield=Decimal(str(data['dividend_yield'])) if data['dividend_yield'] is not None else None,
                    payout_ratio=Decimal(str(data['payout_ratio'])) if data['payout_ratio'] is not None else None,
                    fifty_day_average=Decimal(str(data['fifty_day_average'])),
                    two_hundred_day_average=Decimal(str(data['two_hundred_day_average']))
                )
                
                if ticker in database_ticker_summaries:
                    # Ticker exists - check if data changed
                    existing = database_ticker_summaries[ticker]
                    
                    # Compare key fields to see if update is needed
                    needs_update = (
                        existing.cik != new_summary.cik or
                        existing.market_cap != new_summary.market_cap or
                        existing.previous_close != new_summary.previous_close or
                        existing.pe_ratio != new_summary.pe_ratio or
                        existing.forward_pe_ratio != new_summary.forward_pe_ratio or
                        existing.dividend_yield != new_summary.dividend_yield or
                        existing.payout_ratio != new_summary.payout_ratio or
                        existing.fifty_day_average != new_summary.fifty_day_average or
                        existing.two_hundred_day_average != new_summary.two_hundred_day_average
                    )
                    
                    if needs_update:
                        summaries_to_update.append(new_summary)
                    else:
                        # Unchanged - track it
                        sync_result.unchanged.append(ticker)
                else:
                    # New ticker - add it
                    summaries_to_add.append(new_summary)
                    
            except Exception as e:
                logger.error(f"Error creating TickerSummary for {ticker}: {e}")
                sync_result.failed_ticker_lookups.append(ticker)
        
        # Immediately persist new ticker summaries to database
        if summaries_to_add:
            try:
                added_count = ticker_summary_repo.bulk_insert(summaries_to_add)
                logger.info(f"Batch {batch_num}: Added {added_count} new ticker summaries to database")
                sync_result.to_add.extend(summaries_to_add)
                # Update local cache so subsequent batches see these as existing
                for summary in summaries_to_add:
                    database_ticker_summaries[summary.ticker] = summary
            except Exception as e:
                logger.error(f"Batch {batch_num}: Failed to add ticker summaries: {e}")
                raise
        
        # Immediately persist updated ticker summaries to database
        if summaries_to_update:
            try:
                updated_count = ticker_summary_repo.bulk_update(summaries_to_update)
                logger.info(f"Batch {batch_num}: Updated {updated_count} ticker summaries in database")
                sync_result.to_update.extend(summaries_to_update)
                # Update local cache with new data
                for summary in summaries_to_update:
                    database_ticker_summaries[summary.ticker] = summary
            except Exception as e:
                logger.error(f"Batch {batch_num}: Failed to update ticker summaries: {e}")
                raise
        
        # Add delay between batches to be respectful to the API
        if i + BATCH_SIZE < len(tickers):
            delay = min(2.0, 1.0 + (len(tickers) / 50.0))
            logger.debug(f"Waiting {delay:.1f} seconds before next batch...")
            time.sleep(delay)
    
    logger.info(f"Completed processing all {total_batches} batches")
    logger.info(f"Total: {len(sync_result.to_add)} added, {len(sync_result.to_update)} updated, "
                f"{len(sync_result.unchanged)} unchanged, {len(sync_result.failed_ticker_lookups)} failed lookups")
    
    return sync_result


def identify_tickers_to_delete(
    database_ticker_summaries: Dict[str, TickerSummary],
    processed_tickers: Set[str]
) -> List[str]:
    """
    Identify ticker summaries in database that were not found in the source data.
    These should be deleted as they are no longer valid.
    
    Args:
        database_ticker_summaries: Dictionary of all ticker summaries currently in database
        processed_tickers: Set of ticker symbols that were found in source data
        
    Returns:
        List of ticker symbols to delete from database
    """
    tickers_to_delete: List[str] = []
    
    for ticker in database_ticker_summaries.keys():
        if ticker not in processed_tickers:
            tickers_to_delete.append(ticker)
    
    if tickers_to_delete:
        logger.info(f"Found {len(tickers_to_delete)} ticker summaries in database that are not in source data")
    
    return tickers_to_delete


def delete_obsolete_ticker_summaries(
    ticker_summary_repo: TickerSummaryRepository,
    tickers_to_delete: List[str]
) -> int:
    """
    Delete ticker summaries from database that are no longer in source data.
    
    Args:
        ticker_summary_repo: TickerSummary repository for database operations
        tickers_to_delete: List of ticker symbols to delete
        
    Returns:
        Number of ticker summaries successfully deleted
    """
    if not tickers_to_delete:
        logger.info("No obsolete ticker summaries to delete")
        return 0
    
    logger.info(f"Deleting {len(tickers_to_delete)} obsolete ticker summaries in batches of {BATCH_SIZE}")
    
    total_deleted = 0
    total_batches = (len(tickers_to_delete) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(tickers_to_delete), BATCH_SIZE):
        batch = tickers_to_delete[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        try:
            deleted_count = ticker_summary_repo.bulk_delete(batch)
            total_deleted += deleted_count
            logger.info(f"Delete batch {batch_num}/{total_batches}: Deleted {deleted_count}/{len(batch)} ticker summaries")
        except Exception as e:
            logger.error(f"Delete batch {batch_num}: Failed to delete ticker summaries: {e}")
            raise
    
    logger.info(f"Successfully deleted {total_deleted} obsolete ticker summaries from database")
    return total_deleted
