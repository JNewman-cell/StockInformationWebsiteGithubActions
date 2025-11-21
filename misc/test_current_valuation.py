#!/usr/bin/env python3
"""
Test script for current valuation measures API on AAPL.
"""

import yahooquery as yq
from typing import Any, Dict
import json


def test_current_valuation_measures(ticker: str = "AAPL") -> None:
    """
    Test the current_valuation_measures API for a given ticker.
    
    Args:
        ticker: Stock ticker symbol (default: AAPL)
    """
    print(f"Testing current_valuation_measures API for {ticker}")
    print("=" * 60)
    
    # Create Ticker object
    stock = yq.Ticker(ticker, verify=False, asynchronous=False)
    
    # List all available methods/attributes
    print("\n--- Available methods/attributes (filtered) ---")
    all_attrs = [attr for attr in dir(stock) if not attr.startswith('_')]
    valuation_attrs = [attr for attr in all_attrs if 'valuation' in attr.lower()]
    print(f"Valuation-related: {valuation_attrs}")
    
    financial_attrs = [attr for attr in all_attrs if 'financial' in attr.lower()]
    print(f"Financial-related: {financial_attrs}")
    
    # Check if current_valuation_measures method exists
    has_method = hasattr(stock, 'current_valuation_measures')
    print(f"\nHas 'current_valuation_measures' method: {has_method}")
    
    if not has_method:
        print("❌ Method not found!")
        print("\nTrying alternative: checking key_stats and financial_data...")
        try:
            key_stats = stock.key_stats
            print(f"\n✅ key_stats available!")
            print(f"key_stats keys: {list(key_stats.keys()) if isinstance(key_stats, dict) else 'Not a dict'}")
            if isinstance(key_stats, dict) and ticker in key_stats:
                stats = key_stats[ticker]
                print(f"\nKey stats for {ticker}:")
                print(f"  pegRatio: {stats.get('pegRatio')}")
                print(f"  enterpriseToEbitda: {stats.get('enterpriseToEbitda')}")
                print(f"  priceToBook: {stats.get('priceToBook')}")
        except Exception as e:
            print(f"Error accessing key_stats: {e}")
        return
    
    # Check if it's callable
    current_valuation = getattr(stock, 'current_valuation_measures', None)
    is_callable = callable(current_valuation)
    print(f"Is callable: {is_callable}")
    
    if not is_callable:
        print("❌ Method is not callable!")
        return
    
    # Try to call the method
    try:
        print("\nCalling current_valuation_measures()...")
        val_data = stock.current_valuation_measures()
        
        print(f"\n✅ Successfully retrieved data!")
        print(f"Type: {type(val_data)}")
        
        if isinstance(val_data, dict):
            print(f"Keys: {list(val_data.keys())}")
            
            if ticker in val_data:
                ticker_data = val_data[ticker]
                print(f"\nData for {ticker}:")
                print(json.dumps(ticker_data, indent=2, default=str))
                
                # Check for specific fields
                print("\n--- Key Fields ---")
                print(f"EnterprisesValueEBITDARatio: {ticker_data.get('EnterprisesValueEBITDARatio')}")
                print(f"PbRatio: {ticker_data.get('PbRatio')}")
                print(f"PegRatio: {ticker_data.get('PegRatio')}")
                print(f"EnterpriseValue: {ticker_data.get('EnterpriseValue')}")
                print(f"MarketCap: {ticker_data.get('MarketCap')}")
                
            else:
                print(f"\n⚠️ Ticker '{ticker}' not found in response")
                print("Available tickers:", list(val_data.keys()))
        else:
            print(f"\n⚠️ Unexpected response type: {type(val_data)}")
            print(val_data)
            
    except AttributeError as e:
        print(f"\n❌ AttributeError: {e}")
    except Exception as e:
        print(f"\n❌ Error calling method: {type(e).__name__}: {e}")


if __name__ == "__main__":
    test_current_valuation_measures("AAPL")
