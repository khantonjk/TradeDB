import pandas as pd
from datetime import datetime
from typing import Union, Optional
from portfolio_test.FX_CONSTANTS import currency_conversion_rates


class PortfolioManager:
    """
    Manages portfolio positions and trades using DataFrames instead of a database.
    Records portfolio total value at each trade execution with date as index.

    Supported transaction types: BUY, SELL.

    On initialization the portfolio is seeded with 100 SEK in CASH.
    """

    def __init__(self, seed_date: str = "2020-01-01"):
        """Initialize empty DataFrames for trades and positions."""
        # DataFrame to store all transactions (trade history)
        self.trades_df = pd.DataFrame(
            columns=['transaction_datetime', 'transaction_type', 'ticker', 'shares', 'actual_price', 'currency',
                     'amount']
        )
        self.trades_df['transaction_datetime'] = pd.to_datetime(self.trades_df['transaction_datetime'])

        # DataFrame to store current positions
        self.positions_df = pd.DataFrame(
            columns=['ticker', 'net_shares', 'last_trade_price', 'total_position_value']
        )
        self.positions_df.set_index('ticker', inplace=True)

        # DataFrame to store portfolio value history with date as index
        self.portfolio_value_df = pd.DataFrame(
            columns=['total_value', 'cash_balance', 'stock_value']
        )
        self.portfolio_value_df.index.name = 'date'

        # Seed starting cash (100 SEK)
        self._upsert_position('CASH', 100.0, 1.0, seed_date)
        # Record initial portfolio snapshot
        self._record_portfolio_value(seed_date)

    def _normalize_datetime(self, datetime_input: Union[str, datetime, pd.Timestamp, None]) -> str:
        """
        Helper to ensure transaction dates are always strings in 'YYYY-MM-DD' format.
        Handles None (defaults to today), strings, and pandas Timestamps.
        """
        if datetime_input is None:
            raise ValueError("Transaction datetime must be provided (cannot be None).")
        else:
            dt_obj = pd.to_datetime(datetime_input)

        return dt_obj.strftime('%Y-%m-%d')

    def _convert_price_and_currency_to_sek(self, actual_price: float, currency: Optional[str]) -> tuple[float, str]:
        """
        Converts the actual_price to SEK based on the provided currency.
        """
        currency = currency.upper()
        if currency not in currency_conversion_rates.keys():
            raise ValueError(f"{currency} not supported yet for conversion.")

        if currency != 'SEK':
            conversion_rate = currency_conversion_rates[currency]
            actual_price = round(actual_price * conversion_rate, 4)
            currency = 'SEK'

        return actual_price, currency

    def _upsert_position(self, ticker: str, share_change: float, stock_price: float, tx_datetime: str):
        """
        Updates the positions DataFrame for a specific ticker.
        Handles both Stock and CASH updates.
        """
        # Get existing state
        if ticker in self.positions_df.index:
            current_shares = self.positions_df.loc[ticker, 'net_shares']
        else:
            current_shares = 0.0

        # Calculate New State
        new_shares = current_shares + share_change
        new_price = stock_price if ticker != 'CASH' else 1.0
        new_total_value = new_shares * new_price

        # UPSERT (Insert or Update)
        self.positions_df.loc[ticker, 'net_shares'] = new_shares
        self.positions_df.loc[ticker, 'last_trade_price'] = new_price
        self.positions_df.loc[ticker, 'total_position_value'] = new_total_value

    def _record_portfolio_value(self, tx_datetime: str):
        """
        Calculates and records the total portfolio value with given date as index.
        Updates the value for the date (handles multiple trades per day).
        """
        cash_balance = self.get_cash_balance()
        stock_value = self.positions_df[self.positions_df.index != 'CASH']['total_position_value'].sum()
        total_value = cash_balance + stock_value

        # Convert to date index string (remove any time component)
        date_index = pd.to_datetime(tx_datetime).strftime('%Y-%m-%d')

        # Add or update the portfolio value record for this date
        self.portfolio_value_df.loc[date_index] = [total_value, cash_balance, stock_value]

    def _find_price_for_ticker_in_data(self, ticker: str, data: pd.Series) -> float:
        """
        For 1 single date, find the price for the given ticker in the provided data Series.
        :param ticker: Example AAPL
        """
        token = f"Close ({ticker.upper()})"
        labels = list(data.index)
        if token in labels:
            return data[token]
        else:
            raise ValueError(f"Price for ticker {ticker} not found in data. Expected column like '{token}'."
                             f" Available columns: {labels}")

    def update_portfolio_prices(self, price_data_for_date: pd.Series):
        """
        Updates the prices of all non-cash assets in the portfolio for a given date
        and records the updated portfolio value. This does not generate trade records.

        Args:
            price_data_for_date: A pandas Series containing the latest prices for assets
                                 for a single date. The index of the Series should contain
                                 labels like 'Close (TICKER)' for each asset.
                                 The name of the Series should be the date of the prices.
        """
        if price_data_for_date.empty:
            print("⚠️  No price data provided. Skipping price update.")
            return

        # Extract date from the Series name
        tx_datetime = price_data_for_date.name
        if tx_datetime is None:
            raise ValueError("price_data_for_date Series must have a name representing the date.")

        normalized_datetime = self._normalize_datetime(tx_datetime)

        print(f"Updating portfolio prices for {normalized_datetime}...")

        # Iterate through existing positions and update their values
        for ticker in self.positions_df.index:
            if ticker == 'CASH':
                continue # Skip cash, its value is constant at 1.0 per share

            current_shares = self.positions_df.loc[ticker, 'net_shares']
            if current_shares != 0: # Only update if we actually hold shares or short positions
                try:
                    new_price = self._find_price_for_ticker_in_data(ticker, price_data_for_date)
                    # Update last_trade_price and total_position_value
                    self.positions_df.loc[ticker, 'last_trade_price'] = new_price
                    self.positions_df.loc[ticker, 'total_position_value'] = current_shares * new_price
                except ValueError as e:
                    print(f"⚠️  Could not update price for {ticker} on {normalized_datetime}: {e}")

        # After updating all relevant positions, record the new portfolio value
        self._record_portfolio_value(normalized_datetime)
        print(f"✅ Portfolio value history updated for {normalized_datetime} based on new prices.")

    def record_transaction_percentage_buy_sell(self,
                                               tx_type: str,
                                               ticker: str,
                                               pcnt_of_portfolio: float,
                                               data_one_date: pd.Series,
                                               currency: str = 'SEK'):
        """
        Set amount to buy in percantage of the current portfolio value. For example,
        if the portfolio is worth 1000 SEK and you want to buy 10% of it in AAPL, you would call:
        """
        if not isinstance(data_one_date, pd.Series):
            raise TypeError("data_one_date must be a pandas Series representing price data for the study date.")
        self.update_portfolio_prices(data_one_date)

        # update the price of the ticker for the date of the transaction, so we can calculate the shares to buy/sell

        stock_price = self._find_price_for_ticker_in_data(ticker, data_one_date)

        # Date should be series index
        date = data_one_date.name
        # convert pd Timestamp to string
        date = str(date)
        if date is None:
            raise ValueError("data_one_date must have a valid index representing the date.")
        if tx_type == "BUY" and pcnt_of_portfolio > 0:

            # get current cash balance, see how many shares we can buy with the percentage of the portfolio value
            shares = self.get_cash_balance() * pcnt_of_portfolio / stock_price
        elif tx_type == "SELL" and pcnt_of_portfolio > 0 and not (pcnt_of_portfolio > 1):
            # percentage of ticker position
            if ticker not in self.positions_df.index:
                raise ValueError(f"Ticker {ticker} not found in portfolio positions for SELL transaction.")
            shares = self.positions_df.loc[ticker, 'net_shares'] * pcnt_of_portfolio

        else:
            raise ValueError(
                "Invalid transaction type or percentage (not over 100%). Must be 'BUY' or 'SELL' with positive percentage.")
        # calculate shares to buy
        if shares == 0:
            print(
                f"⚠️  Calculated shares to {tx_type} is 0 for {ticker} at {pcnt_of_portfolio * 100}% of portfolio. "
            )
            if tx_type == "BUY":
                print(f"Current CASH balance: {self.get_cash_balance()}. Price to purchase: {stock_price*shares}.")
            elif tx_type == "SELL":
                print(f"Current shares of {ticker}: {self.positions_df.loc[ticker, 'net_shares']}. ")

            return "Transaction Skipped: Shares to trade is 0"


        # record the transaction. Transactions are recorded as shares of actual price, so the
        # amount is not directly used here, but it is calculated for validation and logging purposes.
        self.record_transaction(
            tx_type=tx_type,
            ticker=ticker,
            shares=shares,
            stock_price=stock_price,
            tx_datetime=date,
            currency=currency
        )

    def record_transaction(self,
                           tx_type: str,
                           ticker: str,
                           shares: float,
                           stock_price: float,
                           tx_datetime: Union[str, datetime, pd.Timestamp, None],
                           currency: str = 'SEK'):
        """
        Records a trade and automatically updates the CASH balance.
        Also records portfolio value snapshot.

        Args:
            tx_type: 'BUY', 'SELL'
            ticker: Stock symbol (e.g. 'AAPL') or 'CASH' for deposits
            shares: Number of shares (float allowed)
            stock_price: Price per share
            tx_datetime: Transaction date/time
            currency: Currency of the actual_price (default 'SEK')
        """
        # Validate transaction type
        if tx_type:
            tx_type = tx_type.upper()
            if tx_type not in ('BUY', 'SELL'):
                raise ValueError("Transaction type must be one of 'BUY' or 'SELL'.")
        else:
            raise ValueError("Transaction type must be provided.")

        # Disallow 'CASH' ticker since DEPOSIT is removed
        if ticker == 'CASH':
            raise ValueError("Ticker 'CASH' is not supported.")

        # Convert currency and normalize datetime
        stock_price, currency = self._convert_price_and_currency_to_sek(stock_price, currency)
        tx_datetime = self._normalize_datetime(tx_datetime)

        total_amount = shares * stock_price

        try:
            # Validate sufficient liquidity for BUY
            if tx_type == 'BUY':
                current_cash = self.get_cash_balance()
                if current_cash < total_amount:
                    print(f"❌ Insufficient cash balance to {tx_type} {total_amount} of {ticker}. "
                          f"Current CASH: {current_cash}")
                    return "Transaction Denied: Insufficient Cash"

            # Validate sufficient shares for SELL
            if tx_type == 'SELL':
                current_stock = self.positions_df.loc[
                    ticker, 'net_shares'] if ticker in self.positions_df.index else 0.0
                if current_stock < shares:
                    print(f"❌ Insufficient shares to SELL {shares} of {ticker}. "
                          f"Current Shares: {current_stock}")
                    return "Transaction Denied: Insufficient Shares"

            # --- 1. Log the Trade ---
            trade_record = {
                'transaction_datetime': tx_datetime,
                'transaction_type': tx_type,
                'ticker': ticker,
                'shares': shares,
                'stock_price': stock_price,
                'currency': currency,
                'amount': total_amount
            }
            self.trades_df = pd.concat([self.trades_df, pd.DataFrame([trade_record])], ignore_index=True)

            # --- 2. Upsert Stock Position ---
            stock_change = shares if tx_type == 'BUY' else -shares
            self._upsert_position(ticker, stock_change, stock_price, tx_datetime)

            # --- 3. Update CASH Balance ---
            if ticker != 'CASH':
                cash_change = 0.0
                if tx_type == 'BUY':
                    cash_change = -total_amount
                elif tx_type == 'SELL':
                    cash_change = total_amount

                if cash_change != 0.0:
                    self._upsert_position('CASH', cash_change, 1.0, tx_datetime)
                    print(f"   -> Cash balance adjusted by {cash_change}")

            # --- 4. Record Portfolio Value ---
            self._record_portfolio_value(tx_datetime)

            print(f"{tx_datetime}: ✅ Recorded {tx_type}: {ticker} @ {total_amount / shares}. Snapshot updated.")

        except Exception as e:
            print(f"❌ Transaction failed: {e}")
            return f"Transaction Denied: {str(e)}"

    def sell_all_assets(self, tx_datetime: Union[str, datetime, pd.Timestamp, None] = None):
        """
        Sells all non-cash assets in the portfolio.
        """
        # Get all tickers with positive shares (excluding CASH)
        assets_to_sell = self.positions_df[
            (self.positions_df.index != 'CASH') & (self.positions_df['net_shares'] > 0)
            ]

        for ticker in assets_to_sell.index:
            shares = self.positions_df.loc[ticker, 'net_shares']
            last_price = self.positions_df.loc[ticker, 'last_trade_price']

            self.record_transaction(
                tx_type='SELL',
                ticker=ticker,
                shares=shares,
                stock_price=last_price,
                tx_datetime=tx_datetime
            )

    # ---------------------------------------------------------
    # Reporting & Getters
    # ---------------------------------------------------------

    def get_cash_balance(self) -> float:
        """Returns current cash balance."""
        if 'CASH' in self.positions_df.index:
            return self.positions_df.loc['CASH', 'net_shares']
        return 0.0

    def get_portfolio_snapshot(self) -> pd.DataFrame:
        """Returns current portfolio positions as DataFrame."""
        snapshot = self.positions_df[self.positions_df['net_shares'] != 0].copy()
        return snapshot.sort_values('total_position_value', ascending=False)

    def get_trades_history(self) -> pd.DataFrame:
        """Returns complete trade history as DataFrame."""
        return self.trades_df.copy()

    def get_portfolio_value_history(self) -> pd.DataFrame:
        """Returns portfolio value history with datetime as index."""
        return self.portfolio_value_df.copy().sort_index()

    def get_current_portfolio_value(self) -> float:
        """Returns the current total portfolio value."""
        if len(self.portfolio_value_df) > 0:
            return self.portfolio_value_df.iloc[-1]['total_value']
        return 0.0

    def set_cash(self, amount: float, tx_datetime: Union[str, datetime, pd.Timestamp, None] = None,
                 currency: str = 'SEK') -> None:
        """
        Set the portfolio cash balance to an absolute amount (replaces current cash).

        This method performs an optional currency conversion to SEK, records a
        'SET_CASH' entry in the trades history for audit, updates the internal
        CASH position, and records a portfolio value snapshot for the provided date.

        Args:
            amount: New cash amount (must be >= 0). If provided in another currency
                    the `currency` argument is used for conversion to SEK.
            tx_datetime: Optional transaction date/time. Defaults to now.
            currency: Currency of the provided amount (default 'SEK').
        """
        if amount is None:
            raise ValueError("Amount must be provided for set_cash.")
        if amount < 0:
            raise ValueError("Cash amount cannot be negative.")

        # Normalize datetime and convert the provided amount to SEK
        tx_datetime = self._normalize_datetime(tx_datetime)
        actual_amount, currency = self._convert_price_and_currency_to_sek(amount, currency)

        # Determine change relative to current cash balance
        current_cash = self.get_cash_balance()
        change = round(actual_amount - current_cash, 4)

        # Log the cash-set operation in trades history for auditing
        trade_record = {
            'transaction_datetime': tx_datetime,
            'transaction_type': 'SET_CASH',
            'ticker': 'CASH',
            'shares': actual_amount,
            'actual_price': 1.0,
            'currency': 'SEK',
            'amount': actual_amount
        }
        self.trades_df = pd.concat([self.trades_df, pd.DataFrame([trade_record])], ignore_index=True)

        # Apply the change to the CASH position
        if change != 0.0:
            self._upsert_position('CASH', change, 1.0, tx_datetime)

        # Record portfolio snapshot
        self._record_portfolio_value(tx_datetime)

        print(f"{tx_datetime}: ✅ Set CASH to {actual_amount}. Change applied: {change}.")
