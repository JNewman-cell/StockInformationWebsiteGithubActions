import yahooquery as yq
import pandas as pd
import psycopg2
import sys
import os
import time
from datetime import datetime
import logging

# Add the project root to the Python path for data layer imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Import data layer components
from data_layer import (
    DatabaseConnectionManager,
    StockRepository,
    Stock,
    StockNotFoundError,
    DuplicateStockError,
    ValidationError,
    DatabaseQueryError
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_database_connectivity(db_manager, stock_repo):
    """Check database connectivity and table structure using data layer."""
    try:
        # Test database connection
        if not db_manager.test_connection():
            logger.error("Database connection test failed")
            return False
        
        logger.info("✓ Database connection successful")
        
        # Test repository functionality by getting count
        # This will validate that the STOCKS table exists and has the correct structure
        try:
            count = stock_repo.count()
            logger.info(f"✓ STOCKS table accessible with {count} existing records")
            return True
        except Exception as e:
            logger.error(f"✗ STOCKS table validation failed: {e}")
            logger.error("Please ensure the STOCKS table exists with the required schema")
            return False
            
    except Exception as e:
        logger.error(f"Database connectivity check failed: {e}")
        return False

def get_batch_ticker_info(symbols_with_exchange, batch_size=50, max_workers=6):
    """Fetch basic info for multiple tickers using yahooquery batch processing.
    
    Args:
        symbols_with_exchange: List of (symbol, exchange) tuples
        batch_size: Number of symbols to process per batch (should be ~8-10x max_workers)
        max_workers: Number of concurrent HTTP requests (keep lower to avoid rate limits)
    """
    all_ticker_data = []
    
    # Process symbols in batches
    for i in range(0, len(symbols_with_exchange), batch_size):
        batch = symbols_with_exchange[i:i + batch_size]
        symbols = [symbol for symbol, exchange in batch]
        
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(symbols_with_exchange) + batch_size - 1)//batch_size} with {len(symbols)} symbols")
        
        try:
            # Create Ticker object with batch of symbols and asynchronous processing
            # Using moderate concurrency to avoid overwhelming Yahoo Finance API
            try:
                stock = yq.Ticker(
                    symbols,
                    asynchronous=True,
                    max_workers=max_workers,
                    progress=True,
                    validate=True,
                    retry=3
                )
            except TypeError as e:
                # Fallback to basic parameters if advanced ones aren't supported
                logger.warning(f"Advanced parameters not supported, using basic configuration: {e}")
                stock = yq.Ticker(symbols, asynchronous=True, validate=True)
            
            # Get basic info for all symbols at once
            summary_data = stock.summary_detail
            profile_data = stock.asset_profile
            
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
                    
                    # Check if there's an error in the data
                    if isinstance(summary_data[symbol], dict) and 'Error Message' in summary_data[symbol]:
                        logger.warning(f"Error in data for {symbol}: {summary_data[symbol]['Error Message']}")
                        continue
                        
                    symbol_info = summary_data[symbol]
                    
                    # Extract market cap to validate the ticker has data
                    market_cap = symbol_info.get('marketCap')
                    if market_cap is None or market_cap == 0:
                        logger.warning(f"No market cap available for symbol: {symbol}")
                        continue
                        
                    # Get company name from profile data
                    company_name = None
                    if (symbol in profile_data and 
                        profile_data[symbol] is not None and
                        not isinstance(profile_data[symbol], dict) or 
                        'Error Message' not in profile_data[symbol]):
                        
                        profile_info = profile_data[symbol]
                        if isinstance(profile_info, dict):
                            company_name = profile_info.get('longName')
                    
                    ticker_data = {
                        'symbol': symbol,
                        'company': company_name,
                        'exchange': exchange,
                        'market_cap': market_cap  # We'll use this for validation but not store it
                    }
                    
                    all_ticker_data.append(ticker_data)
                    logger.debug(f"Successfully processed symbol: {symbol}")
                    
                except Exception as e:
                    logger.error(f"Error processing symbol {symbol}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            # Fall back to individual processing for this batch if batch processing fails
            logger.info("Falling back to individual symbol processing for this batch")
            for symbol, exchange in batch:
                individual_data = get_individual_ticker_info(symbol, exchange)
                if individual_data:
                    all_ticker_data.append(individual_data)
            
        # Add a delay between batches to be respectful to the API
        # Longer delay for larger batches or when we've processed many symbols
        if i + batch_size < len(symbols_with_exchange):
            delay = min(2.0, 1.0 + (len(symbols) / 50.0))  # Scale delay with batch size
            logger.debug(f"Waiting {delay:.1f} seconds before next batch...")
            time.sleep(delay)
    
    return all_ticker_data

def get_individual_ticker_info(ticker, exchange=None):
    """Fetch basic info for a single ticker (fallback method)."""
    try:
        # Use basic ticker creation to avoid parameter conflicts
        stock = yq.Ticker(ticker)
        
        # Get basic info
        info = stock.summary_detail
        profile = stock.asset_profile
        
        if ticker not in info or info[ticker] is None:
            logger.warning(f"No data available for ticker: {ticker}")
            return None
            
        ticker_info = info[ticker]
        
        # Extract market cap to validate the ticker has data
        market_cap = ticker_info.get('marketCap')
        if market_cap is None or market_cap == 0:
            logger.warning(f"No market cap available for ticker: {ticker}")
            return None
            
        # Get company name
        company_name = None
        if ticker in profile and profile[ticker] is not None:
            profile_data = profile[ticker]
            company_name = profile_data.get('longName')
        
        return {
            'symbol': ticker,
            'company': company_name,
            'exchange': exchange,
            'market_cap': market_cap  # We'll use this for validation but not store it
        }
        
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return None

def process_stocks_batch(stock_repo, ticker_batch):
    """Process a batch of ticker data using the data layer."""
    successful_updates = 0
    failed_updates = 0
    
    # Convert ticker data to Stock objects
    stocks_to_process = []
    
    for ticker_data in ticker_batch:
        try:
            # Create Stock object with validation
            stock = Stock(
                symbol=ticker_data['symbol'],
                company=ticker_data.get('company'),
                exchange=ticker_data.get('exchange')
            )
            stocks_to_process.append(stock)
        except ValidationError as e:
            logger.warning(f"Skipping invalid stock data for {ticker_data.get('symbol', 'UNKNOWN')}: {e}")
            failed_updates += 1
            continue
    
    # Use bulk insert with conflict resolution (upsert behavior)
    if stocks_to_process:
        try:
            created_stocks = stock_repo.bulk_insert(stocks_to_process)
            successful_updates = len(created_stocks)
            logger.info(f"Bulk processed {successful_updates} stocks successfully")
        except Exception as e:
            logger.error(f"Bulk insert failed, falling back to individual processing: {e}")
            # Fall back to individual processing
            for stock in stocks_to_process:
                try:
                    # Try to create, if it exists, update it
                    existing_stock = stock_repo.get_by_symbol(stock.symbol)
                    if existing_stock:
                        # Update existing stock
                        existing_stock.company = stock.company
                        existing_stock.exchange = stock.exchange
                        stock_repo.update(existing_stock)
                        logger.debug(f"Updated existing stock: {stock.symbol}")
                    else:
                        # Create new stock
                        stock_repo.create(stock)
                        logger.debug(f"Created new stock: {stock.symbol}")
                    
                    successful_updates += 1
                    
                except Exception as e:
                    logger.error(f"Error processing individual stock {stock.symbol}: {e}")
                    failed_updates += 1
    
    return successful_updates, failed_updates

def process_ticker_file(file_path):
    """Process a ticker file and return list of tickers with exchange info."""
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
                if ticker and len(ticker) <= 20:  # Basic validation for symbol length
                    tickers.append((ticker, exchange))
        logger.info(f"Loaded {len(tickers)} tickers from {file_path} (exchange: {exchange})")
        return tickers
    except Exception as e:
        logger.error(f"Error reading ticker file {file_path}: {e}")
        return []

def main():
    """Main function to process tickers and update database using data layer."""
    if len(sys.argv) < 2:
        logger.error("Usage: python create_stocks_table.py <ticker_file1> [ticker_file2] ...")
        sys.exit(1)
    
    # Initialize data layer components
    try:
        logger.info("Initializing data layer...")
        db_manager = DatabaseConnectionManager()  # Uses DATABASE_URL from environment
        stock_repo = StockRepository(db_manager)
        
        # Check database connectivity and table structure
        if not check_database_connectivity(db_manager, stock_repo):
            logger.error("Cannot proceed without proper database setup.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Failed to initialize data layer: {e}")
        sys.exit(1)
    
    try:
        # Process all ticker files
        all_tickers = []
        for file_path in sys.argv[1:]:
            if os.path.exists(file_path):
                tickers = process_ticker_file(file_path)
                all_tickers.extend(tickers)
            else:
                logger.warning(f"Ticker file not found: {file_path}")
        
        if not all_tickers:
            logger.error("No ticker files found or no tickers loaded")
            sys.exit(1)
        
        # Remove duplicates (keep first occurrence with exchange info)
        seen_symbols = set()
        unique_tickers = []
        for symbol, exchange in all_tickers:
            if symbol not in seen_symbols:
                unique_tickers.append((symbol, exchange))
                seen_symbols.add(symbol)
        
        logger.info(f"Processing {len(unique_tickers)} unique symbols using batch processing")
        
        # Use batch processing to get ticker data with optimized batch size
        # Batch size of 50 with 6 workers = ~8 symbols per worker, which is more reasonable
        all_ticker_data = get_batch_ticker_info(unique_tickers, batch_size=50, max_workers=6)
        
        logger.info(f"Retrieved data for {len(all_ticker_data)} symbols with valid market cap data")
        
        # Process tickers in batches for better performance and error handling
        batch_size = 100  # Process database operations in batches of 100
        total_successful = 0
        total_failed = 0
        
        for i in range(0, len(all_ticker_data), batch_size):
            batch = all_ticker_data[i:i + batch_size]
            logger.info(f"Processing database batch {i//batch_size + 1}/{(len(all_ticker_data) + batch_size - 1)//batch_size} ({len(batch)} stocks)")
            
            batch_successful, batch_failed = process_stocks_batch(stock_repo, batch)
            total_successful += batch_successful
            total_failed += batch_failed
        
        # Calculate skipped symbols
        skipped_symbols = len(unique_tickers) - len(all_ticker_data)
        logger.info(f"Skipped {skipped_symbols} symbols due to missing market cap data or API errors")
        
        # Final summary
        logger.info(f"Processing complete. Successful: {total_successful}, Failed: {total_failed}")
        print(f"Processing complete. Successfully processed {total_successful} symbols.")
        
        # Get final database statistics
        try:
            final_count = stock_repo.count()
            exchanges = stock_repo.get_exchanges()
            logger.info(f"Final database statistics: {final_count} total stocks across {len(exchanges)} exchanges")
            logger.info(f"Exchanges: {', '.join(exchanges) if exchanges else 'None'}")
        except Exception as e:
            logger.warning(f"Could not retrieve final statistics: {e}")
    
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Clean up database connections
        try:
            db_manager.close_all_connections()
            logger.info("Database connections closed")
        except Exception as e:
            logger.warning(f"Error closing database connections: {e}")

if __name__ == "__main__":
    main()