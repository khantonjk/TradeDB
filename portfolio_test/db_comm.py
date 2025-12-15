# python
import sqlite3
from datetime import datetime
from typing import List, Dict, Union

DATABASE_NAME = 'trades.db'


class PortfolioDBManager:
    """
    Manages all database connections and operations for the portfolio tracker.
    Handles inserting transactions into the Trades log and managing the
    Current_Positions snapshot table.
    """

    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conn = self._connect_db()

    def _connect_db(self) -> sqlite3.Connection:
        """Establishes and returns a database connection."""
        try:
            conn = sqlite3.connect(self.db_name)
            # Use Row factory to access columns by name instead of index
            conn.row_factory = sqlite3.Row
            # Enable foreign key enforcement
            conn.execute("PRAGMA foreign_keys = ON;")
            return conn
        except sqlite3.Error as e:
            print(f"Database connection error: {e}")
            raise

    def _update_current_positions(self, ticker: str, tx_type: str, shares: float, actual_price: float,
                                  tx_datetime: str):
        """
        Internal method to manage the Current_Positions snapshot table (the Python-managed "trigger").
        """
        cursor = self.conn.cursor()

        # 1. Determine the share change (the inverse of the operation)
        share_change = shares if tx_type in ('BUY', 'DEPOSIT') else -shares

        # 2. Get existing state to calculate new state
        cursor.execute("SELECT net_shares, last_trade_price FROM Current_Positions WHERE ticker = ?", (ticker,))
        current_pos = cursor.fetchone()

        # Set initial values for calculation if the position doesn't exist
        current_net_shares = current_pos['net_shares'] if current_pos else 0.0

        # Use the existing price or the new price if it's a stock
        current_last_price = current_pos['last_trade_price'] if current_pos else 0.0

        # Calculate NEW state
        new_net_shares = current_net_shares + share_change

        # Only update price if it's a stock trade
        new_last_price = actual_price if ticker != 'CASH' else current_last_price

        # Total value is the new quantity times the new/current last price
        new_total_value = new_net_shares * new_last_price

        # 3. Insert or Update the Current_Positions table (UPSERT)
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
                           ticker, new_net_shares, new_last_price, new_total_value, tx_datetime,  # Insert values
                           new_net_shares, new_last_price, new_total_value, tx_datetime  # Update values
                       ))

    def record_transaction(self, tx_type: str, ticker: str, shares: float, actual_price: float,
                           valued_price: float = None, currency: str = 'SEK'):
        """
        Inserts a new transaction into the Trades log and updates the Current_Positions snapshot.
        """
        tx_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        total_amount = round(shares * actual_price, 4)

        cursor = self.conn.cursor()

        # 1. Insert into Trades (Log)
        cursor.execute("""
                       INSERT INTO Trades (transaction_datetime, transaction_type, ticker, shares, actual_price,
                                           valued_price, currency, amount)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       """, (
                           tx_datetime, tx_type, ticker, shares, actual_price, valued_price, currency, total_amount
                       ))

        # 2. Update Current_Positions (Snapshot)
        self._update_current_positions(ticker, tx_type, shares, actual_price, tx_datetime)

        self.conn.commit()
        print(f"✅ Recorded {tx_type} of {shares} {ticker} and updated snapshot.")

    def get_portfolio_snapshot(self) -> List[Dict[str, Union[str, float]]]:
        """
        Retrieves all current positions (stocks and cash) and their calculated values
        from the fast Current_Positions table.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT ticker,
                              net_shares,
                              last_trade_price,
                              total_position_value
                       FROM Current_Positions
                       WHERE net_shares != 0
                       ORDER BY total_position_value DESC
                       """)

        # Convert sqlite3.Row objects to standard dictionaries for easy use
        return [dict(row) for row in cursor.fetchall()]

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
