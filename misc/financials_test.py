from yahooquery import Ticker
from typing import Any, Dict
import inspect


def _call_financial_method(ticker_obj: Ticker, method_name: str, quarter: bool = False, trailing: bool = True) -> Any:
    """Call a yahooquery financial method and handle variations in params.

    yahooquery used to accept qtr=True for quarterly data; newer versions use
    frequency='q' (and 'a' for annual). Try to detect supported args from
    the method signature and call the method accordingly so this script works
    with multiple yahooquery versions.
    """
    method = getattr(ticker_obj, method_name)
    sig = inspect.signature(method)
    params = sig.parameters

    kwargs = {}
    if "frequency" in params:
        kwargs["frequency"] = "q" if quarter else "a"
    if "qtr" in params:
        kwargs["qtr"] = quarter
    if "trailing" in params:
        kwargs["trailing"] = trailing

    if kwargs:
        return method(**kwargs)
    else:
        # fallback: attempt to call with only the default signature
        return method()


def get_full_history(symbol: str) -> Dict[str, Any]:
    t = Ticker(symbol)

    return {
        "annual_income": _call_financial_method(t, "income_statement", quarter=False),
        "quarterly_income": _call_financial_method(t, "income_statement", quarter=True),
        "annual_balance": _call_financial_method(t, "balance_sheet", quarter=False),
        "quarterly_balance": _call_financial_method(t, "balance_sheet", quarter=True),
        "annual_cashflow": _call_financial_method(t, "cash_flow", quarter=False),
        "quarterly_cashflow": _call_financial_method(t, "cash_flow", quarter=True),
    }


if __name__ == "__main__":
    data = get_full_history("AAPL")
    # print a short summary so the script is useful when run directly
    print(data)
    for k, v in data.items():
        rows = len(v.index) if hasattr(v, "index") else "unknown"
        print(f"{k}: rows={rows}")

        # print column titles if the object looks like a DataFrame
        if hasattr(v, "columns"):
            try:
                cols = list(v.columns)
                print(f"  columns ({len(cols)}): {cols}")
            except Exception:
                print("  columns: [unavailable]")
        else:
            print("  columns: [no columns attribute]")
