# python
import sqlite3
from datetime import datetime

DATABASE_NAME = 'trades.db'

# --- Initial Data ---
INITIAL_CASH_SEK = 100.00
deposit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def create_and_initialize_database():
    """
    Creates the two-table structure (Trades log and Current_Positions snapshot)
    and inserts the initial cash balance.
    """
    conn = None
    try:
        # 1. Setup Connection
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        # Enable foreign key enforcement (good practice)
        cursor.execute("PRAGMA foreign_keys = ON;")

        print(f"--- Setting up database '{DATABASE_NAME}' ---")

        # 2. Drop Tables and Trigger (for clean recreation)
        cursor.execute("DROP TABLE IF EXISTS Current_Positions")
        cursor.execute("DROP TABLE IF EXISTS Trades")
        cursor.execute("DROP TRIGGER IF EXISTS update_positions_after_trade")
        print("Existing tables and trigger dropped.")

        # --- 3. Create Tables ---

        print("Creating 'Trades' table (The Log)...")
        cursor.execute('''
                       CREATE TABLE Trades
                       (
                           id                   INTEGER PRIMARY KEY,
                           transaction_datetime DATETIME NOT NULL,
                           transaction_type     TEXT     NOT NULL CHECK (transaction_type IN ('BUY', 'SELL', 'DEPOSIT', 'WITHDRAW')),
                           ticker               TEXT     NOT NULL,
                           shares               REAL     NOT NULL,
                           actual_price         REAL     NOT NULL,
                           currency             TEXT,
                           amount               REAL     NOT NULL
                       )
                       ''')

        print("Creating 'Current_Positions' table (The Snapshot)...")
        cursor.execute('''
                       CREATE TABLE Current_Positions
                       (
                           ticker               TEXT PRIMARY KEY,
                           net_shares           REAL     NOT NULL,
                           last_trade_price     REAL     NOT NULL,
                           total_position_value REAL     NOT NULL,
                           last_updated         DATETIME NOT NULL
                       )
                       ''')

        conn.commit()
        print("Database structure successfully created and initialized.")

    except sqlite3.Error as e:
        print(f"\n❌ An ERROR occurred during database setup:\n{e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    create_and_initialize_database()