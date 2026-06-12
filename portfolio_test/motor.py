import pandas as pd
import yfinance as yf

from portfolio_test.FX_CONSTANTS import currency_conversion_rates


class CalculationMotor(yf.Ticker):
    def __init__(self, ticker: str, start: str = None, end: str = None, convert_currency: bool = True):
        # initialize the parent yf.Ticker
        super().__init__(ticker)
        self.start = self._start_date(start)
        self.end = self._end_date(end)
        # fetch price history, date ascending as default
        self.df = self.history(start=self.start, end=self.end, auto_adjust=False)
        self.df.index = self.df.index.normalize().tz_localize(None)
        
        if convert_currency:
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

    def get_daily_pe_ratio(self) -> pd.Series:
        """
        Estimates the daily P/E ratio by taking the daily 'Close' price 
        and dividing it by the most recently reported annual EPS.
        Since YFinance only reports earnings annually, this forward-fills 
        the EPS to give a daily PE estimate.
        """
        income_stmt = self.income_stmt
        
        if 'Basic EPS' not in income_stmt.index and 'Diluted EPS' not in income_stmt.index:
            raise ValueError(f"EPS data not found in income statement for {self.ticker}.")
            
        eps_key = 'Basic EPS' if 'Basic EPS' in income_stmt.index else 'Diluted EPS'
        
        # Extract the EPS row, sort by date ascending, and convert to numeric
        eps_series = pd.to_numeric(income_stmt.loc[eps_key]).sort_index(ascending=True)
        eps_series = self._convert_to_sek(eps_series, self.history_metadata['currency'])
        
        # Align EPS with daily price data
        daily_prices = self.df['Adj Close']
        eps_df = pd.DataFrame({'EPS': eps_series})
        prices_df = pd.DataFrame({'Adj Close': daily_prices})
        
        # Outer join ensures we don't lose EPS data reported on non-trading days
        combined = prices_df.join(eps_df, how='outer')
        
        # Forward-fill previous earnings, back-fill for dates prior to first report
        combined['EPS'] = combined['EPS'].ffill().bfill()
        
        # Keep only valid trading days and compute PE
        combined = combined.dropna(subset=['Adj Close'])
        
        # Prevent division by zero
        combined['EPS'] = combined['EPS'].replace(0, pd.NA)
        
        pe_ratio = combined['Adj Close'] / combined['EPS']
        pe_ratio.name = f"PE_Ratio ({self.ticker})"
        
        return pe_ratio