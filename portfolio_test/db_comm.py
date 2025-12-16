import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Union, Optional
from FX_CONSTANTS import currency_conversion_rates
# Import your existing motor class
# Assuming motor.py is in the same directory
from motor import CalculationMotor

DATABASE_NAME = 'trades.db'


class PortfolioDBManager:
    """
    Manages database connections, records trades, automatically handles cash
    adjustments, and integrates with CalculationMotor for historical pricing.
    """

    def __init__(self, db_name: str = DATABASE_NAME):
        self.db_name = db_name
        self.conn = self._connect_db()

    def _connect_db(self) -> sqlite3.Connection:
        """Establishes and returns a database connection."""
        try:
            conn = sqlite3.connect(self.db_name)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON;")
            return conn
        except sqlite3.Error as e:
            print(f"Database connection error: {e}")
            raise

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    # ---------------------------------------------------------
    # Core Database Updates
    # ---------------------------------------------------------

    def _upsert_position(self, ticker: str, share_change: float, price: float, tx_datetime: str):
        """
        Updates the Current_Positions table for a specific ticker.
        Handles both Stock and CASH updates.
        """
        cursor = self.conn.cursor()

        # 1. Get existing state
        cursor.execute("SELECT net_shares, last_trade_price FROM Current_Positions WHERE ticker = ?", (ticker,))
        row = cursor.fetchone()

        current_shares = row['net_shares'] if row else 0.0
        # For CASH, the "price" is always 1.0. For stocks, use last known or new price.
        if ticker == 'CASH':
            last_price = 1.0
        else:
            last_price = row['last_trade_price'] if row else price

        # 2. Calculate New State
        new_shares = current_shares + share_change

        # If it's a stock trade, update the last_trade_price to the new transaction price
        # If it's a CASH update, price remains 1.0
        new_price = price if ticker != 'CASH' else 1.0

        new_total_value = new_shares * new_price

        # 3. UPSERT (Insert or Update)
        cursor.execute("""
                       INSERT INTO Current_Positions (ticker, net_shares, last_trade_price, total_position_value,
                                                      last_updated)
                       VALUES (?, ?, ?, ?, ?) ON CONFLICT(ticker) DO
                       UPDATE SET
                           net_shares = ?,
                           last_trade_price = ?,
                           total_position_value = ?,
                           last_updated = ?
                       """, (
                           ticker, new_shares, new_price, new_total_value, tx_datetime,
                           new_shares, new_price, new_total_value, tx_datetime
                       ))

    def record_transaction(self,
                           tx_type: str,
                           ticker: str,
                           shares: float,
                           actual_price: float,
                           tx_datetime: str = None,
                           currency: str = 'SEK'):
        """
        Records a trade and automatically updates the CASH balance.

        Args:
            tx_type: 'BUY', 'SELL', 'DEPOSIT', 'WITHDRAW'
            ticker: Stock symbol (e.g. 'AAPL') or 'CASH' for deposits
            shares: Number of shares (float allowed)
            actual_price: Price per share
            tx_datetime: Optional 'YYYY-MM-DD HH:MM:SS' string. Defaults to now.
        """
        if tx_type:
            tx_type = tx_type.upper()
            if tx_type in ('BUY', 'SELL', 'DEPOSIT', 'WITHDRAW'):
                pass
        else:
            raise ValueError("Transaction type must be provided and be one of 'BUY', 'SELL', 'DEPOSIT', 'WITHDRAW'.")

        if ticker == "CASH" and tx_type not in ('DEPOSIT', 'WITHDRAW'):
            raise ValueError("For 'CASH' ticker, transaction type must be 'DEPOSIT' or 'WITHDRAW'.")
        elif ticker == "CASH" and tx_type in ('DEPOSIT', 'WITHDRAW'):
            # For CASH deposits/withdrawals, shares represent the amount directly
            shares = shares  # amount in currency units
            actual_price = 1.0  # Price per share is always 1 for CASH

        if currency is not None:
            currency = currency.upper()
            if currency not in currency_conversion_rates.keys():
                raise ValueError(f"{currency} not supported yet for conversion.")
            else:
                # Convert actual_price to SEK if needed
                if currency != 'SEK':
                    conversion_rate = currency_conversion_rates[currency]
                    actual_price = round(actual_price * conversion_rate, 4)
                    currency = 'SEK'  # After conversion, we store as SEK

        if tx_datetime is None:
            tx_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        total_amount = round(shares * actual_price, 4)
        cursor = self.conn.cursor()

        try:

            # fail transaction if missing liquidity for BUY or WITHDRAW
            if tx_type == 'BUY' or tx_type == 'WITHDRAW':
                # get current cash balance
                cursor.execute("SELECT net_shares FROM Current_Positions WHERE ticker = 'CASH'")
                row = cursor.fetchone()
                current_cash = row['net_shares'] if row else 0.0
                if current_cash < total_amount:
                    print(f"❌ Insufficient cash balance to {tx_type} {total_amount} of {ticker}. "
                          f"Current CASH: {current_cash}")
                    return "Transaction Denied: Insufficient Cash"

            # --- 1. Log the Trade ---
            cursor.execute("""
                           INSERT INTO Trades (transaction_datetime, transaction_type, ticker, shares, actual_price,
                                               currency, amount)
                           VALUES (?, ?, ?, ?, ?, ?, ?)
                           """, (tx_datetime, tx_type, ticker, shares, actual_price, currency, total_amount))

            # --- 2. Update Stock Position ---
            # Determine direction for the stock (BUY adds shares, SELL removes shares)
            stock_change = shares if tx_type in ('BUY', 'DEPOSIT') else -shares
            self._upsert_position(ticker, stock_change, actual_price, tx_datetime)

            # --- 3. Update CASH Balance (The "Motor" Logic) ---
            # If we bought stock, Cash goes DOWN. If we sold stock, Cash goes UP.
            # Deposits increase cash, Withdrawals decrease cash.

            if ticker != 'CASH':
                cash_change = 0.0
                if tx_type == 'BUY':
                    cash_change = -total_amount  # Spend money
                elif tx_type == 'SELL':
                    cash_change = total_amount  # Receive money

                if cash_change != 0.0:
                    self._upsert_position('CASH', cash_change, 1.0, tx_datetime)
                    print(f"   -> Cash balance adjusted by {cash_change}")

            self.conn.commit()
            print(f"✅ Recorded {tx_type}: {shares} {ticker} @ {actual_price}. Snapshot updated.")

        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"❌ Transaction failed: {e}")


    # helper function to deposit cash
    def deposit_cash(self, amount: float, tx_datetime: str = None):
        """
        Deposits cash into the portfolio.

        Args:
            amount: Amount to deposit
            tx_datetime: Optional 'YYYY-MM-DD HH:MM:SS' string. Defaults to now.
        """
        self.record_transaction(
            tx_type='DEPOSIT',
            ticker='CASH',
            shares=amount,
            actual_price=1.0,
            tx_datetime=tx_datetime
        )




    # ---------------------------------------------------------
    # Reporting
    # ---------------------------------------------------------

    def get_portfolio_snapshot(self) -> pd.DataFrame:
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT ticker, net_shares, last_trade_price, total_position_value
                       FROM Current_Positions
                       WHERE net_shares != 0
                       ORDER BY total_position_value DESC
                       """)
        positions_rows = [dict(row) for row in cursor.fetchall()]
        return pd.DataFrame(positions_rows)
