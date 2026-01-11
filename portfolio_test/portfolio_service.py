import pandas as pd


#
# Need to code this.
# a service to measure the performance of the portfolio
#
class PortfolioService:
    def __init__(self, portfolio_manager, data_forge_df: pd.DataFrame):
        """
        :param portfolio_manager: Instance of your DB class.
        :param data_forge_df: DataFrame where Index=Date and Columns=Price data, with the asset
        type in parentheses for example "Close (AAPL)", "Close (MSFT)", "PE (AAPL)", etc.
        """
        self.pm = portfolio_manager
        self.df = self._validate_data_forge(data_forge_df)

    def _validate_data_forge(self, df):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("data_forge_df must be a pandas.DataFrame or an object with .market_data")

        if df.empty:
            raise ValueError("data_forge_df is empty")

        # Confirm index is date
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index, errors='raise')
                df.index = pd.DatetimeIndex(df.index).normalize()
                df = df.sort_index(ascending=True)
            except Exception as exc:
                raise TypeError("data_forge_df must have a DateTime index (could not parse index)",
                                type(df.index)) from exc
        return df

    def get_total_valuation(self) -> float:
        """
        Get the current portfolio valuation using the latest available market prices from
        `self.df`. This implementation:
        - expects a pandas DataFrame (or an object with `.market_data` DataFrame)
        - finds the latest row and picks the best matching "Close (TICKER)" column per ticker
        - warns and treats missing tickers as 0
        """
        df = self.df.copy()

        latest_date_row = df.iloc[-1]

        portfolio_df = self.pm.get_portfolio_snapshot()
        if portfolio_df.empty:
            raise ValueError("Portfolio snapshot is empty")

        # Helper to find the best matching column for a ticker
        def _find_price_column_for_ticker(ticker: str):
            token = f"({ticker.upper()})"
            candidates = [col for col in df.columns if token in col.upper()]
            if not candidates:
                return None
            # prefer columns containing 'Close' (case-insensitive)
            close_price_candidates = [c for c in candidates if 'CLOSE' in c.upper()]
            if len(close_price_candidates) > 1:
                raise ValueError(f"Multiple close price columns found for ticker {ticker}: {close_price_candidates}")
            elif len(close_price_candidates) == 1:
                return close_price_candidates[0]  # close price column
            else:
                return candidates[0]  # no 'Close' column; return first candidate

        # Build a mapping ticker -> latest price
        price_map = {}
        if 'ticker' not in portfolio_df.columns:
            raise KeyError("portfolio snapshot must contain a 'ticker' column")
        if 'net_shares' not in portfolio_df.columns:
            raise KeyError("portfolio snapshot must contain a 'net_shares' column")
        if 'last_trade_price' not in portfolio_df.columns:
            raise KeyError("portfolio snapshot must contain a 'last_trade_price' column")

        for t in portfolio_df['ticker'].unique():
            # if t == 'CASH': # skip cash entry
            #    continue
            col = _find_price_column_for_ticker(t)
            if col is None:  # no price column found in market data
                price_map[t] = portfolio_df[portfolio_df['ticker'] == t]['last_trade_price'].iloc[0]
            else:
                # use .get to avoid KeyError if column missing in latest_date_row
                latest_price = latest_date_row.get(col, None)
                if pd.isna(latest_price):  # missing price data for this ticker but column exists
                    price_map[t] = portfolio_df[portfolio_df['ticker'] == t]['last_trade_price'].iloc[0]
                else:  # valid price found
                    print(f"Latest price for {t} from column {col} is {round(latest_price, 2)}")
                    price_map[t] = latest_price

        # Append current_price column to a copy of portfolio_df
        portfolio_df = portfolio_df.copy()
        portfolio_df['current_price'] = portfolio_df['ticker'].map(price_map)

        # Calculate total value: sum(shares * price) + cash
        shares_value_and_cash_value = (portfolio_df['net_shares'] * portfolio_df['current_price']).sum()
        # cash_value = self.pm.get_cash_balance()
        # return #float(shares_value + cash_value)
        return shares_value_and_cash_value

    def get_sharpe_ratio(self, column_name: str, risk_free_rate: float = 0.02) -> float:
        """
        Calculate the Sharpe Ratio of the portfolio based on daily returns.
        :param risk_free_rate: Annual risk-free rate (default is 2%)
        :return: Sharpe Ratio
        """
        df = self.df.copy()

        # Calculate daily returns
        daily_returns = df[column_name].pct_change().dropna()

        # Calculate average daily return and standard deviation of daily returns
        avg_daily_return = daily_returns.mean()
        std_daily_return = daily_returns.std() # = variance**0.5

        # Annualize the average return and standard deviation
        trading_days_per_year = 252
        annualized_return = avg_daily_return * trading_days_per_year
        annualized_std = std_daily_return * (trading_days_per_year ** 0.5)

        # Calculate Sharpe Ratio
        sharpe_ratio = (annualized_return - risk_free_rate) / annualized_std

        return sharpe_ratio.mean()  # Return the mean Sharpe Ratio across all assets
