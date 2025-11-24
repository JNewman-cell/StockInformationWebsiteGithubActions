#!/usr/bin/env python3
import json
import yahooquery as yq  # type: ignore

# Print summary_detail for a list of tickers
TICKERS = ['AAPL', 'INTU', 'MSFT']

stock = yq.Ticker(TICKERS, verify=False, asynchronous=False)
summary = stock.summary_detail

for ticker in TICKERS:
    key = ticker.upper()
    data = summary.get(key)
    print('---')
    print('Ticker:', key)
    if not isinstance(data, dict):
        print('No data or data is not a dict: ', data)
        continue
    print(json.dumps({
        'dividendYield': data.get('dividendYield'),
        'trailingAnnualDividendYield': data.get('trailingAnnualDividendYield'),
        'dividendRate': data.get('dividendRate'),
        'trailingAnnualDividendRate': data.get('trailingAnnualDividendRate'),
        'regularMarketPreviousClose': data.get('regularMarketPreviousClose'),
        'payoutRatio': data.get('payoutRatio')
    }, default=str, indent=2))
