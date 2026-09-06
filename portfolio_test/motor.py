import pandas as pd
import numpy as np
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

    def get_daily_returns(self) -> pd.Series:
        """Returns the daily percentage return Series of Adj Close."""
        returns = self.df['Adj Close'].pct_change().dropna()
        returns.name = f"Returns ({self.ticker})"
        return returns

    def get_daily_rolling_volatility(self, window: int = 60, annualized: bool = True) -> pd.Series:
        """
        Calculates rolling volatility over a specified window.
        If annualized=True, scales by sqrt(252).
        """
        returns = self.get_daily_returns()
        min_p = max(5, window // 4)
        vol = returns.rolling(window=window, min_periods=min_p).std()
        if annualized:
            vol = vol * np.sqrt(252)
        vol.name = f"Rolling_Vol_{window}d ({self.ticker})"
        return vol

    def get_daily_rolling_var(self, window: int = 60, confidence_level: float = 0.95) -> pd.Series:
        """
        Calculates daily historical Value at Risk (VaR) over a rolling window.
        Returned as a positive percentage loss (e.g. 0.02 = 2% max expected loss at 95% confidence).
        """
        returns = self.get_daily_returns()
        min_p = max(5, window // 4)
        alpha = 1.0 - confidence_level
        rolling_q = returns.rolling(window=window, min_periods=min_p).quantile(alpha)
        var_series = rolling_q.apply(lambda x: -x if x < 0 else 0.0)
        var_series.name = f"Rolling_VaR_{int(confidence_level*100)}_{window}d ({self.ticker})"
        return var_series

    def get_daily_rolling_cvar(self, window: int = 60, confidence_level: float = 0.95) -> pd.Series:
        """
        Calculates daily Conditional Value at Risk (CVaR) / Expected Shortfall over a rolling window.
        Measures the average loss during the worst (1 - confidence_level) tail days.
        """
        returns = self.get_daily_returns()
        min_p = max(5, window // 4)
        alpha = 1.0 - confidence_level

        def _calc_cvar(s):
            q = s.quantile(alpha)
            tail = s[s <= q]
            return -tail.mean() if not tail.empty else -q

        cvar_series = returns.rolling(window=window, min_periods=min_p).apply(_calc_cvar, raw=False)
        cvar_series.name = f"Rolling_CVaR_{int(confidence_level*100)}_{window}d ({self.ticker})"
        return cvar_series

    def get_daily_rolling_beta(self, benchmark_series: pd.Series, window: int = 60) -> pd.Series:
        """
        Calculates daily rolling Beta relative to a benchmark series over a rolling window.
        """
        asset_returns = self.get_daily_returns()
        bench_returns = benchmark_series.pct_change().dropna()
        
        # Normalize indices
        bench_returns.index = pd.to_datetime(bench_returns.index).normalize()
        asset_returns.index = pd.to_datetime(asset_returns.index).normalize()
        
        aligned_bench = bench_returns.reindex(asset_returns.index)
        min_p = max(5, window // 4)
        
        rolling_cov = asset_returns.rolling(window=window, min_periods=min_p).cov(aligned_bench)
        rolling_bench_var = aligned_bench.rolling(window=window, min_periods=min_p).var()
        
        beta_series = rolling_cov / rolling_bench_var.replace(0, np.nan)
        beta_series.name = f"Rolling_Beta_{window}d ({self.ticker})"
        return beta_series

    def get_daily_rolling_risk_metrics(self, benchmark_series: pd.Series = None, window: int = 60, confidence_level: float = 0.95) -> pd.DataFrame:
        """
        Returns a DataFrame containing all daily rolling risk metrics for the asset.
        """
        df = pd.DataFrame(index=self.df.index)
        df[f"Adj_Close ({self.ticker})"] = self.df['Adj Close']
        df[f"Returns ({self.ticker})"] = self.get_daily_returns()
        df[f"Rolling_Vol_{window}d ({self.ticker})"] = self.get_daily_rolling_volatility(window=window)
        df[f"Rolling_VaR_{int(confidence_level*100)}_{window}d ({self.ticker})"] = self.get_daily_rolling_var(window=window, confidence_level=confidence_level)
        df[f"Rolling_CVaR_{int(confidence_level*100)}_{window}d ({self.ticker})"] = self.get_daily_rolling_cvar(window=window, confidence_level=confidence_level)

        if benchmark_series is not None:
            df[f"Rolling_Beta_{window}d ({self.ticker})"] = self.get_daily_rolling_beta(benchmark_series, window=window)

        return df