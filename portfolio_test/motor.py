import yfinance as yf
import pandas as pd

class CalculationMotor(yf.Ticker):
    def __init__(self, ticker: str, start: str = None, end: str = None):
        # initialize the parent yf.Ticker
        super().__init__(ticker)
        self.start = start
        self.end = end
        # fetch price history once (optional)
        self.prices = self.history(start=self.start, end=self.end, auto_adjust=False)

    def refresh_history(self, start: str = None, end: str = None):
        start = start or self.start
        end = end or self.end
        self.prices = self.history(start=start, end=end, auto_adjust=False)
        return self.prices

    def get_trailing_eps(self) -> float | None:
        # use existing .info attribute from yf.Ticker
        try:
            return float((self.info or {}).get('trailingEps'))
        except Exception:
            return None

    def get_quarterly_earnings(self) -> pd.DataFrame:
        # use existing .quarterly_earnings attribute
        qe = getattr(self, 'quarterly_earnings', None)
        return pd.DataFrame(qe) if qe is not None else pd.DataFrame()

    # Add higher-level helpers that use yf.Ticker internals
    def compute_simple_pe_series(self) -> pd.Series | None:
        if self.prices is None or self.prices.empty:
            return None
        trailing_eps = self.get_trailing_eps()
        if trailing_eps is None or trailing_eps == 0:
            return None
        return self.prices['Close'] / float(trailing_eps)