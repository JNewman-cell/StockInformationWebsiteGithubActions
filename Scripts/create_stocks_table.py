import yahooquery as yq
import pandas as pd
import psycopg2
import sys
import os
import time
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database_connection(connection_string):
    """Create a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(connection_string)
        logger.info("Successfully connected to the database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def check_stocks_table(conn):
    """Check if the stocks table exists and has the expected structure."""
    check_table_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'stocks'
    );
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            cursor.close()
            logger.error("Stocks table does not exist in the database")
            return False
        
        # Check if the table has the expected columns
        check_columns_query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'stocks' AND table_schema = 'public'
        ORDER BY ordinal_position;
        """
        
        cursor.execute(check_columns_query)
        columns = cursor.fetchall()
        cursor.close()
        
        expected_columns = ['id', 'symbol', 'company', 'exchange', 'created_at', 'last_updated_at']
        actual_columns = [col[0] for col in columns]
        
        missing_columns = [col for col in expected_columns if col not in actual_columns]
        if missing_columns:
            logger.error(f"Stocks table is missing required columns: {missing_columns}")
            return False
        
        logger.info(f"Stocks table exists with columns: {actual_columns}")
        return True
        
    except Exception as e:
        logger.error(f"Error checking table: {e}")
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
            stock = yq.Ticker(
                symbols,
                asynchronous=True,
                max_workers=max_workers,
                progress=True,
                validate=True,
                retry=3,
                timeout=15,
                backoff_factor=0.5  # Slower backoff to be more respectful
            )
            
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
        stock = yq.Ticker(ticker, retry=2, timeout=5)
        
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

def insert_or_update_ticker(conn, ticker_data):
    """Insert or update ticker data in the database."""
    insert_query = """
    INSERT INTO stocks (symbol, company, exchange, created_at, last_updated_at)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (symbol) 
    DO UPDATE SET 
        company = EXCLUDED.company,
        exchange = EXCLUDED.exchange,
        last_updated_at = EXCLUDED.last_updated_at;
    """
    
    try:
        cursor = conn.cursor()
        current_time = datetime.now()
        cursor.execute(insert_query, (
            ticker_data['symbol'],
            ticker_data['company'],
            ticker_data['exchange'],
            current_time,
            current_time
        ))
        conn.commit()
        cursor.close()
        logger.info(f"Successfully inserted/updated symbol: {ticker_data['symbol']}")
        return True
    except Exception as e:
        logger.error(f"Error inserting/updating symbol {ticker_data['symbol']}: {e}")
        return False

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
    """Main function to process tickers and update database."""
    if len(sys.argv) < 2:
        logger.error("Usage: python create_stocks_table.py <ticker_file1> [ticker_file2] ...")
        sys.exit(1)
    
    # Get database connection string from environment
    connection_string = os.getenv('DATABASE_URL')
    if not connection_string:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)
    
    # Connect to database
    conn = create_database_connection(connection_string)
    if not conn:
        sys.exit(1)
    
    # Check that the stocks table exists
    if not check_stocks_table(conn):
        conn.close()
        logger.error("Cannot proceed without the stocks table. Please ensure it exists in the database.")
        sys.exit(1)
    
    # Process all ticker files
    all_tickers = []
    for file_path in sys.argv[1:]:
        if os.path.exists(file_path):
            tickers = process_ticker_file(file_path)
            all_tickers.extend(tickers)
        else:
            logger.warning(f"Ticker file not found: {file_path}")
    
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
    
    # Process each ticker for database insertion
    successful_updates = 0
    failed_updates = 0
    
    for ticker_data in all_ticker_data:
        # Insert/update in database
        if insert_or_update_ticker(conn, ticker_data):
            successful_updates += 1
        else:
            failed_updates += 1
    
    # Calculate skipped symbols
    skipped_symbols = len(unique_tickers) - len(all_ticker_data)
    logger.info(f"Skipped {skipped_symbols} symbols due to missing market cap data or API errors")
    
    # Close database connection
    conn.close()
    
    logger.info(f"Processing complete. Successful: {successful_updates}, Failed: {failed_updates}")
    print(f"Processing complete. Successfully processed {successful_updates} symbols.")

if __name__ == "__main__":
    main()