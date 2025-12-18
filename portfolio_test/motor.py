import pandas as pd
import yfinance as yf

from portfolio_test.FX_CONSTANTS import currency_conversion_rates


class CalculationMotor(yf.Ticker):
    def __init__(self, ticker: str, start: str = None, end: str = None):
        # initialize the parent yf.Ticker
        super().__init__(ticker)
        self.start = self._start_date(start)
        self.end = self._end_date(end)
        # fetch price history once
        self.df = self.history(start=self.start, end=self.end, auto_adjust=False)
        self.df.index = self.df.index.normalize().tz_localize(None)
        self.df["Open"] = self._convert_to_sek(self.df["Open"], self.history_metadata['currency'])
        self.df["Close"] = self._convert_to_sek(self.df["Close"], self.history_metadata['currency'])
        self.df["High"] = self._convert_to_sek(self.df["High"], self.history_metadata['currency'])
        self.df["Low"] = self._convert_to_sek(self.df["Low"], self.history_metadata['currency'])
        self.df["Adj Close"] = self._convert_to_sek(self.df["Adj Close"], self.history_metadata['currency'])


    def _convert_to_sek(self, price: pd.Series, currency: str) -> pd.Series:
        currency = currency.upper()
        if currency not in currency_conversion_rates.keys():
            raise ValueError(f"{currency} not supported yet for conversion.")

        conversion_rate = currency_conversion_rates[currency]
        return price * conversion_rate

    def _start_date(self, start) -> str:
        """Internal method to determine start date."""
        if start is not None:
            return start
        return "2020-01-01"

    def _end_date(self, end) -> str:
        """Internal method to determine end date."""
        if end is not None:
            return end
        return pd.Timestamp.today().strftime('%Y-%m-%d')

