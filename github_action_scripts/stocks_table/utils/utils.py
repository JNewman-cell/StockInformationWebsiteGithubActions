"""
Utility functions for stock table synchronization.
"""

import logging
import os
import time
import requests
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple, Optional
import yahooquery as yq

logger = logging.getLogger(__name__)


def fetch_ticker_data_from_github_repo():
    """Fetch ticker data directly from the Improved-US-Stock-Symbols GitHub repository.
    
    Uses the 'all_tickers.json' file which contains symbols from all exchanges.
    Since we can't determine exchange from the combined list, we'll use a generic 'US' exchange.
    
    Returns:
        List of (normalized_symbol, exchange) tuples
    """
    tickers = []
    
    # URL for the all tickers JSON file - contains symbols from all US exchanges
    url = 'https://raw.githubusercontent.com/JNewman-cell/Improved-US-Stock-Symbols/main/all/all_tickers.json'
    
    try:
        logger.info("Fetching all US ticker data from GitHub repository...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse JSON response - should be a simple array of ticker symbols
        ticker_symbols = response.json()
        
        if not isinstance(ticker_symbols, list):
            logger.error(f"Unexpected JSON format: expected list, got {type(ticker_symbols)}")
            raise RuntimeError("Invalid JSON format received from GitHub repository")
            
        # Process each ticker symbol
        for ticker in ticker_symbols:
            if isinstance(ticker, str) and ticker.strip():
                # Normalize ticker by replacing / and \ with - to follow Yahoo Finance conventions
                normalized_ticker = ticker.strip().upper().replace('/', '-').replace('\\', '-')
                # Filter out tickers longer than 6 characters (likely invalid)
                if len(normalized_ticker) <= 6:
                    # Use 'US' as exchange since the all_tickers.json doesn't specify individual exchanges
                    tickers.append((normalized_ticker, 'US'))
        
        logger.info(f"Successfully loaded {len(tickers)} ticker symbols from GitHub repository")
        
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


def _extract_error_message(item):
    """Return an error message string if the response item contains an error.

    Handles legacy 'Error Message' keys and the newer structured 'error' object
    returned by the Yahoo endpoint. Returns None when no error is found.
    
    Expected new structure: {'EAI': {'error': {'code': 404, 'type': 'NotFoundError', 
                                               'message': '...', 'symbol': 'EAI'}}}
    """
    if not isinstance(item, dict):
        return None
    # Legacy payload
    if 'Error Message' in item:
        return item.get('Error Message')
    # New structured error payload: {"error": {"code":.., "message":..., "symbol":...}}
    if 'error' in item and isinstance(item['error'], dict):
        return item['error'].get('message') or item['error'].get('type')
    return None


def parse_ticker_symbols_from_exchange_file(file_path):
    """Process a ticker file and return list of tickers with exchange info.
    
    Normalizes ticker symbols by replacing forward slashes (/) and backslashes (\) 
    with hyphens (-) to follow Yahoo Finance API conventions.
    
    Args:
        file_path: Path to the ticker file
        
    Returns:
        List of (normalized_symbol, exchange) tuples
    """
    tickers = []
    # Determine exchange from filename
    exchange = None
    if 'nyse' in file_path.lower():
        exchange = 'NYSE'
    elif 'nasdaq' in file_path.lower():
        exchange = 'NASDAQ'
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                ticker = line.strip().upper()
                if ticker and len(ticker) <= 6:
                    # Normalize ticker by replacing / and \ with - to follow Yahoo Finance conventions
                    normalized_ticker = ticker.replace('/', '-').replace('\\', '-')
                    tickers.append((normalized_ticker, exchange))
        logger.info(f"Loaded {len(tickers)} tickers from {file_path} (exchange: {exchange})")
        return tickers
    except Exception as e:
        logger.error(f"Error reading ticker file {file_path}: {e}")
        return []


def fetch_individual_stock_data_from_yahoo_finance(symbol, exchange):
    """Fallback function to get individual ticker info when batch processing fails.
    
    Args:
        symbol: Stock symbol
        exchange: Exchange name
        
    Returns:
        Dict with ticker data or None if failed
    """
    try:
        stock = yq.Ticker(symbol)
        summary_data = stock.summary_detail
        profile_data = stock.asset_profile
        
        if symbol not in summary_data or summary_data[symbol] is None:
            return None

        # Detect structured or legacy error messages in the summary payload
        summary_err = None
        if isinstance(summary_data[symbol], dict):
            summary_err = _extract_error_message(summary_data[symbol])
        if summary_err:
            logger.debug(f"Error fetching summary data from yahoo for {symbol}: {summary_err}")
            return None

        symbol_info = summary_data[symbol]
        market_cap = symbol_info.get('marketCap')
        
        if market_cap is None or market_cap == 0:
            return None
            
        # Get company name from multiple sources (yahooquery fork has different structure)
        company_name = None
        
        # Try quote_type first (most reliable for company names)
        try:
            quote_type_data = stock.quote_type
            if (symbol in quote_type_data and 
                quote_type_data[symbol] is not None and
                isinstance(quote_type_data[symbol], dict)):
                company_name = quote_type_data[symbol].get('longName') or quote_type_data[symbol].get('shortName')
        except:
            pass
        
        # Fallback to price data if quote_type failed
        if not company_name:
            try:
                price_data = stock.price
                if (symbol in price_data and 
                    price_data[symbol] is not None and
                    isinstance(price_data[symbol], dict)):
                    company_name = price_data[symbol].get('longName') or price_data[symbol].get('shortName')
            except:
                pass
        
        # Last resort: check profile_data (though it doesn't seem to have longName in this fork)
        if not company_name:
            if (symbol in profile_data and 
                profile_data[symbol] is not None and
                isinstance(profile_data[symbol], dict)):

                profile_err = _extract_error_message(profile_data[symbol])
                if not profile_err:
                    profile_info = profile_data[symbol]
                    company_name = profile_info.get('longName')
                else:
                    logger.debug(f"Error fetching profile data from yahoo for {symbol}: {profile_err}")
        
        return {
            'symbol': symbol,
            'company': company_name,
            'exchange': exchange,
            'market_cap': market_cap
        }
        
    except Exception as e:
        logger.error(f"Error getting individual ticker info for {symbol}: {e}")
        return None


def fetch_and_validate_stocks_from_yahoo_finance_api(symbols_with_exchange, process_stocks_batch_func, batch_size=50, max_workers=6):
    """Fetch basic info for multiple tickers using yahooquery batch processing.
    Process and persist each batch immediately to reduce memory usage and provide incremental progress.
    
    Args:
        symbols_with_exchange: List of (symbol, exchange) tuples
        process_stocks_batch_func: Function to process a batch of stocks
        batch_size: Number of symbols to process per batch (should be ~8-10x max_workers)
        max_workers: Number of concurrent HTTP requests (keep lower to avoid rate limits)
    
    Returns:
        Tuple of (total_successful, total_failed, total_skipped)
    """
    total_successful = 0
    total_failed = 0
    total_skipped = 0
    
    # Process symbols in batches
    for i in range(0, len(symbols_with_exchange), batch_size):
        batch = symbols_with_exchange[i:i + batch_size]
        symbols = [symbol for symbol, exchange in batch]
        
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(symbols_with_exchange) + batch_size - 1)//batch_size} with {len(symbols)} symbols")
        
        batch_ticker_data = []  # Collect data for this batch only
        
        try:
            # Create Ticker object with batch of symbols and asynchronous processing
            # Using moderate concurrency to avoid overwhelming Yahoo Finance API
            try:
                stock = yq.Ticker(
                    symbols,
                    asynchronous=True,
                    max_workers=max_workers,
                    progress=True,
                    validate=True
                )
            except TypeError as e:
                # Fallback to basic parameters if advanced ones aren't supported
                logger.warning(f"Advanced parameters not supported, using basic configuration: {e}")
                stock = yq.Ticker(symbols, asynchronous=True, validate=True)
            
            # Get basic info for all symbols at once
            summary_data = stock.summary_detail
            profile_data = stock.asset_profile
            
            # Get company name data from correct sources
            quote_type_data = None
            price_data = None
            try:
                quote_type_data = stock.quote_type
                price_data = stock.price
            except Exception as e:
                logger.debug(f"Could not get company name data: {e}")
            
            # Check for invalid symbols
            if hasattr(stock, 'invalid_symbols') and stock.invalid_symbols:
                logger.warning(f"Invalid symbols found: {stock.invalid_symbols}")
            
            # Process each symbol in the batch
            for symbol, exchange in batch:
                try:
                    # Skip if symbol was marked as invalid
                    if hasattr(stock, 'invalid_symbols') and symbol in stock.invalid_symbols:
                        logger.warning(f"Skipping invalid symbol: {symbol}")
                        continue
                    
                    # Check if we have summary data for this symbol
                    if symbol not in summary_data or summary_data[symbol] is None:
                        logger.warning(f"No summary data available for symbol: {symbol}")
                        continue
                    
                    # Check if there's an error in the data (legacy or structured)
                    summary_err = None
                    if isinstance(summary_data[symbol], dict):
                        summary_err = _extract_error_message(summary_data[symbol])
                    if summary_err:
                        logger.warning(f"Error fetching summary data from yahoo for {symbol}: {summary_err}")
                        continue
                        
                    symbol_info = summary_data[symbol]
                    
                    # Extract market cap to validate the ticker has data
                    market_cap = symbol_info.get('marketCap')
                    if market_cap is None or market_cap == 0:
                        logger.warning(f"No market cap available for symbol: {symbol}")
                        continue
                        
                    # Get company name from multiple sources (yahooquery fork has different structure)
                    company_name = None
                    
                    # Try quote_type first (most reliable for company names)
                    if quote_type_data and symbol in quote_type_data and isinstance(quote_type_data[symbol], dict):
                        company_name = quote_type_data[symbol].get('longName') or quote_type_data[symbol].get('shortName')
                    
                    # Fallback to price data if quote_type failed
                    if not company_name and price_data and symbol in price_data and isinstance(price_data[symbol], dict):
                        company_name = price_data[symbol].get('longName') or price_data[symbol].get('shortName')
                    
                    # Last resort: check profile_data (though it doesn't seem to have longName in this fork)
                    if not company_name:
                        if (symbol in profile_data and 
                            profile_data[symbol] is not None and
                            isinstance(profile_data[symbol], dict)):

                            profile_err = _extract_error_message(profile_data[symbol])
                            if not profile_err:
                                profile_info = profile_data[symbol]
                                company_name = profile_info.get('longName')
                            else:
                                logger.debug(f"Error fetching profile data from yahoo for {symbol} during batch processing: {profile_err}")
                    
                    ticker_data = {
                        'symbol': symbol,
                        'company': company_name,
                        'exchange': exchange,
                        'market_cap': market_cap  # We'll use this for validation but not store it
                    }
                    
                    batch_ticker_data.append(ticker_data)
                    logger.debug(f"Successfully processed symbol: {symbol}")
                    
                except Exception as e:
                    logger.error(f"Error processing symbol {symbol}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            # Fall back to individual processing for this batch if batch processing fails
            logger.info("Falling back to individual symbol processing for this batch")
            for symbol, exchange in batch:
                individual_data = fetch_individual_stock_data_from_yahoo_finance(symbol, exchange)
                if individual_data:
                    batch_ticker_data.append(individual_data)
        
        # Process this batch immediately if we have data
        if batch_ticker_data:
            logger.info(f"Persisting {len(batch_ticker_data)} stocks from current batch to database")
            batch_successful, batch_failed = process_stocks_batch_func(batch_ticker_data)
            total_successful += batch_successful
            total_failed += batch_failed
            
            # Log batch results
            logger.info(f"Batch {i//batch_size + 1} results: {batch_successful} successful, {batch_failed} failed")
        else:
            logger.warning(f"No valid data found in batch {i//batch_size + 1}")
        
        # Calculate skipped symbols for this batch
        batch_skipped = len(batch) - len(batch_ticker_data)
        total_skipped += batch_skipped
        
        # Add a delay between batches to be respectful to the API
        # Longer delay for larger batches or when we've processed many symbols
        if i + batch_size < len(symbols_with_exchange):
            delay = min(2.0, 1.0 + (len(symbols) / 50.0))  # Scale delay with batch size
            logger.debug(f"Waiting {delay:.1f} seconds before next batch...")
            time.sleep(delay)
    
    return total_successful, total_failed, total_skipped


def load_and_deduplicate_ticker_symbols_from_files(file_paths, process_ticker_file_func):
    """Load and process ticker files.
    
    Args:
        file_paths: List of ticker file paths
        process_ticker_file_func: Function to process individual ticker files
        
    Returns:
        List of unique (symbol, exchange) tuples
    """
    all_tickers = []
    
    for file_path in file_paths:
        if os.path.exists(file_path):
            tickers = process_ticker_file_func(file_path)
            all_tickers.extend(tickers)
        else:
            logger.warning(f"Ticker file not found: {file_path}")
    
    if not all_tickers:
        raise ValueError("No ticker files found or no tickers loaded")
    
    # Remove duplicates (keep first occurrence with exchange info)
    seen_symbols = set()
    unique_tickers = []
    for symbol, exchange in all_tickers:
        if symbol not in seen_symbols:
            unique_tickers.append((symbol, exchange))
            seen_symbols.add(symbol)
    
    logger.info(f"Loaded {len(unique_tickers)} unique symbols from {len(file_paths)} files")
    return unique_tickers


def validate_stock_symbols_market_cap_via_yahoo_finance_api(symbols_to_add: List[Tuple[str, str]], batch_size: int = 50) -> Tuple[List[Dict], List[str]]:
    """
    Validate new stocks using Yahoo Finance API to ensure they have valid data.
    
    Args:
        symbols_to_add: List of (symbol, exchange) tuples to validate
        batch_size: Number of symbols to process per batch
        
    Returns:
        Tuple of (valid_stock_data_list, failed_symbols_list)
    """
    if not symbols_to_add:
        return [], []
    
    valid_stocks = []
    failed_symbols = []
    
    logger.info(f"Validating {len(symbols_to_add)} new stocks with Yahoo Finance API")
    
    # Process in batches to avoid overwhelming the API
    for i in range(0, len(symbols_to_add), batch_size):
        batch = symbols_to_add[i:i + batch_size]
        symbols = [symbol for symbol, exchange in batch]
        
        logger.info(f"Validating batch {i//batch_size + 1}/{(len(symbols_to_add) + batch_size - 1)//batch_size}")
        
        try:
            # Use Yahoo Finance API to get stock data
            stock = yq.Ticker(symbols, asynchronous=True, validate=True)
            summary_data = stock.summary_detail
            profile_data = stock.asset_profile
            
            # Get company name data from correct sources
            quote_type_data = None
            price_data = None
            try:
                quote_type_data = stock.quote_type
                price_data = stock.price
            except Exception as e:
                logger.debug(f"Could not get company name data: {e}")
            
            # Check for invalid symbols
            if hasattr(stock, 'invalid_symbols') and stock.invalid_symbols:
                failed_symbols.extend(stock.invalid_symbols)
                logger.warning(f"Invalid symbols found: {stock.invalid_symbols}")
            
            # Validate each symbol in the batch
            for symbol, exchange in batch:
                if symbol in failed_symbols:
                    continue
                
                try:
                    # Check if we have valid summary data
                    if (symbol not in summary_data or 
                        summary_data[symbol] is None):
                        failed_symbols.append(symbol)
                        logger.warning(f"No valid data for symbol: {symbol}")
                        continue
                    # Check for structured/legacy error
                    if isinstance(summary_data[symbol], dict):
                        summary_err = _extract_error_message(summary_data[symbol])
                        if summary_err:
                            failed_symbols.append(symbol)
                            logger.warning(f"Error fetching summary data from yahoo for {symbol} during validation: {summary_err}")
                            continue
                    
                    symbol_info = summary_data[symbol]
                    market_cap = symbol_info.get('marketCap')
                    
                    # Ensure the stock has a valid market cap
                    if market_cap is None or market_cap == 0:
                        failed_symbols.append(symbol)
                        logger.warning(f"No market cap available for symbol: {symbol}")
                        continue
                    
                    # Get company name from multiple sources (yahooquery fork has different structure)
                    company_name = None
                    
                    # Try quote_type first (most reliable for company names)
                    if quote_type_data and symbol in quote_type_data and isinstance(quote_type_data[symbol], dict):
                        company_name = quote_type_data[symbol].get('longName') or quote_type_data[symbol].get('shortName')
                    
                    # Fallback to price data if quote_type failed
                    if not company_name and price_data and symbol in price_data and isinstance(price_data[symbol], dict):
                        company_name = price_data[symbol].get('longName') or price_data[symbol].get('shortName')
                    
                    # Last resort: check profile_data (though it doesn't seem to have longName in this fork)
                    if not company_name:
                        if (symbol in profile_data and 
                            profile_data[symbol] is not None and
                            isinstance(profile_data[symbol], dict)):

                            profile_err = _extract_error_message(profile_data[symbol])
                            if not profile_err:
                                profile_info = profile_data[symbol]
                                company_name = profile_info.get('longName')
                            else:
                                logger.debug(f"Error fetching profile data from yahoo for {symbol} during validation: {profile_err}")
                    
                    # Stock is valid, add to valid list
                    valid_stock_data = {
                        'symbol': symbol,
                        'company': company_name,
                        'exchange': exchange,
                        'market_cap': market_cap
                    }
                    valid_stocks.append(valid_stock_data)
                    logger.debug(f"Validated stock: {symbol}")
                    
                except Exception as e:
                    failed_symbols.append(symbol)
                    logger.error(f"Error validating symbol {symbol}: {e}")
            
        except Exception as e:
            logger.error(f"Error processing validation batch: {e}")
            # Fall back to individual processing for this batch
            logger.info("Falling back to individual validation for this batch")
            for symbol, exchange in batch:
                if symbol in failed_symbols:
                    continue
                
                individual_data = fetch_individual_stock_data_from_yahoo_finance(symbol, exchange)
                if individual_data:
                    valid_stocks.append(individual_data)
                else:
                    failed_symbols.append(symbol)
    
    logger.info(f"Validation complete: {len(valid_stocks)} valid, {len(failed_symbols)} failed")
    return valid_stocks, failed_symbols


def compare_existing_stocks_with_yahoo_finance_data_for_updates(stocks_to_check: List, update_batch_func=None, batch_size: int = 50) -> Tuple[List, List[str]]:
    """
    Check if existing stocks need updates by comparing with Yahoo Finance data.
    Process and update stocks in batches immediately to avoid memory issues.
    
    Args:
        stocks_to_check: List of Stock objects to check
        update_batch_func: Optional function to call with batches of stocks that need updates
        batch_size: Number of stocks to process per batch
        
    Returns:
        Tuple of (stocks_needing_updates, failed_symbols)
        Note: If update_batch_func is provided, stocks_needing_updates will be empty 
              since updates are processed immediately
    """
    if not stocks_to_check:
        return [], []
    
    stocks_needing_updates = []
    failed_symbols = []
    total_updated = 0
    
    logger.info(f"Checking {len(stocks_to_check)} stocks for potential updates")
    
    # Process in batches
    for i in range(0, len(stocks_to_check), batch_size):
        batch = stocks_to_check[i:i + batch_size]
        symbols = [stock.symbol for stock in batch]
        
        logger.info(f"Checking batch {i//batch_size + 1}/{(len(stocks_to_check) + batch_size - 1)//batch_size}")
        
        try:
            # Get current data from Yahoo Finance
            stock_yahoo = yq.Ticker(symbols, asynchronous=True, validate=True)
            profile_data = stock_yahoo.asset_profile
            
            # Check for errors in profile_data
            if symbols:  # Check if we have symbols to process
                for symbol in symbols:
                    if (symbol in profile_data and 
                        profile_data[symbol] is not None and
                        isinstance(profile_data[symbol], dict)):
                        profile_err = _extract_error_message(profile_data[symbol])
                        if profile_err:
                            logger.warning(f"Error fetching profile data from yahoo for {symbol} during update check: {profile_err}")
                            failed_symbols.append(symbol)
            
            # Get company name data from correct sources
            quote_type_data = None
            price_data = None
            try:
                quote_type_data = stock_yahoo.quote_type
                price_data = stock_yahoo.price
            except Exception as e:
                logger.debug(f"Could not get company name data for update check: {e}")
            
            # Check each stock in the batch
            for stock in batch:
                symbol = stock.symbol
                
                try:
                    # Get company name from multiple sources (yahooquery fork has different structure)
                    yahoo_company_name = None
                    
                    # Try quote_type first (most reliable for company names)
                    if quote_type_data and symbol in quote_type_data and isinstance(quote_type_data[symbol], dict):
                        yahoo_company_name = quote_type_data[symbol].get('longName') or quote_type_data[symbol].get('shortName')
                    
                    # Fallback to price data if quote_type failed
                    if not yahoo_company_name and price_data and symbol in price_data and isinstance(price_data[symbol], dict):
                        yahoo_company_name = price_data[symbol].get('longName') or price_data[symbol].get('shortName')
                    
                    # Last resort: check profile_data (with proper error checking)
                    if not yahoo_company_name:
                        if (symbol in profile_data and 
                            profile_data[symbol] is not None and
                            isinstance(profile_data[symbol], dict)):
                            
                            profile_err = _extract_error_message(profile_data[symbol])
                            if not profile_err:
                                profile_info = profile_data[symbol]
                                yahoo_company_name = profile_info.get('longName')
                            else:
                                logger.debug(f"Error fetching profile data for {symbol} in update check: {profile_err}")
                        else:
                            # No valid profile data available
                            failed_symbols.append(symbol)
                            continue
                    
                    # Compare company name to see if update is needed
                    needs_update = False
                    
                    # If we don't have a company name but Yahoo does
                    if stock.company is None and yahoo_company_name:
                        needs_update = True
                        stock.company = yahoo_company_name
                    
                    # If company names are different
                    elif stock.company != yahoo_company_name and yahoo_company_name:
                        needs_update = True
                        stock.company = yahoo_company_name
                    
                    if needs_update:
                        # Update the last_updated_at timestamp
                        stock.last_updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                        stocks_needing_updates.append(stock)
                        logger.debug(f"Stock {symbol} needs update")
                    
                except Exception as e:
                    failed_symbols.append(symbol)
                    logger.error(f"Error checking stock {symbol} for updates: {e}")
            
            # Process updates for this batch immediately if callback provided
            if update_batch_func and stocks_needing_updates:
                batch_updates = []
                for stock in stocks_needing_updates:
                    if stock in batch:  # Only process stocks from current batch
                        batch_updates.append(stock)
                
                if batch_updates:
                    logger.info(f"Updating {len(batch_updates)} stocks from batch {i//batch_size + 1}")
                    try:
                        updated_count = update_batch_func(batch_updates)
                        total_updated += updated_count
                        logger.info(f"Successfully updated {updated_count} stocks from current batch")
                        # Remove processed stocks from the main list since they're already updated
                        stocks_needing_updates = [s for s in stocks_needing_updates if s not in batch_updates]
                    except Exception as e:
                        logger.error(f"Error in batch update for batch {i//batch_size + 1}: {e}")
            
        except Exception as e:
            logger.error(f"Error processing update check batch: {e}")
            # Fall back to individual processing
            for stock in batch:
                try:
                    individual_data = fetch_individual_stock_data_from_yahoo_finance(stock.symbol, stock.exchange)
                    if individual_data and individual_data.get('company') != stock.company:
                        stock.company = individual_data.get('company')
                        stock.last_updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                        stocks_needing_updates.append(stock)
                except Exception as e:
                    failed_symbols.append(stock.symbol)
                    logger.error(f"Error in individual update check for {stock.symbol}: {e}")
    
    if update_batch_func:
        logger.info(f"Update check complete: {total_updated} stocks updated immediately, {len(failed_symbols)} failed")
        # Return empty list since updates were processed immediately
        return [], failed_symbols
    else:
        logger.info(f"Update check complete: {len(stocks_needing_updates)} need updates, {len(failed_symbols)} failed")
        return stocks_needing_updates, failed_symbols


def print_final_statistics(stock_repo, total_successful, total_failed, total_skipped):
    """Print final processing statistics.
    
    Args:
        stock_repo: Stock repository instance
        total_successful: Number of successfully processed symbols
        total_failed: Number of failed symbols
        total_skipped: Number of skipped symbols
    """
    # Final summary
    logger.info(f"Processing complete. Successful: {total_successful}, Failed: {total_failed}, Skipped: {total_skipped}")
    print(f"Processing complete. Successfully processed {total_successful} symbols.")
    
    # Get final database statistics
    try:
        final_count = stock_repo.count()
        exchanges = stock_repo.get_exchanges()
        logger.info(f"Final database statistics: {final_count} total stocks across {len(exchanges)} exchanges")
        logger.info(f"Exchanges: {', '.join(exchanges) if exchanges else 'None'}")
    except Exception as e:
        logger.warning(f"Could not retrieve final statistics: {e}")
