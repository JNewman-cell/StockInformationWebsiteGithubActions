#!/usr/bin/env python3
"""
Simple test script to verify the new fetch_ticker_data_from_github_repo function works correctly.
"""

import sys
import os

# Add the path to the utils module
sys.path.append(os.path.join(os.path.dirname(__file__), 'github_action_scripts', 'stocks_table'))

from utils.utils import fetch_ticker_data_from_github_repo

def test_fetch_tickers():
    """Test the new ticker fetching function."""
    try:
        print("Testing fetch_ticker_data_from_github_repo()...")
        
        # Fetch ticker data
        tickers = fetch_ticker_data_from_github_repo()
        
        print(f"✅ Successfully fetched {len(tickers)} tickers")
        
        # Show first 10 tickers as examples
        print("\nFirst 10 tickers:")
        for i, (symbol, exchange) in enumerate(tickers[:10]):
            print(f"  {i+1}. {symbol} ({exchange})")
        
        # Show some statistics
        print(f"\nStatistics:")
        print(f"  Total tickers: {len(tickers)}")
        print(f"  Unique symbols: {len(set(symbol for symbol, _ in tickers))}")
        
        # Check for some well-known symbols
        symbols = {symbol for symbol, _ in tickers}
        well_known = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        found_symbols = [s for s in well_known if s in symbols]
        print(f"  Well-known symbols found: {found_symbols}")
        
        if len(found_symbols) >= 3:
            print("✅ Test passed - found expected major symbols")
            return True
        else:
            print("❌ Test failed - missing expected major symbols")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False

if __name__ == "__main__":
    success = test_fetch_tickers()
    sys.exit(0 if success else 1)