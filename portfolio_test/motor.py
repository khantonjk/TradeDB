import yfinance as yf
import pandas as pd
import numpy as np
import warnings


class CalculationMotor(yf.Ticker):
    def __init__(self, ticker: str, start: str = None, end: str = None):
        # initialize the parent yf.Ticker
        super().__init__(ticker)
        self.start = self._start_date(start)
        self.end = self._end_date(end)
        # fetch price history once
        self.df = self.history(start=self.start, end=self.end, auto_adjust=False)


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



