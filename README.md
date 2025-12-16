# TradeDB: Quantitative Strategy Backtester with Transaction Logging
TradeMotor is a custom-built Python framework designed to backtest trading 
strategies using real-world data. It combines a calculation engine (wrapping the Yahoo Finance API) with a persistent 
SQLite database to track portfolio performance and log transactions.

This project bridges the gap between simple data fetching and actual portfolio tracking, 
allowing you to simulate trades, manage a cash balance, and store your trading history permanently.

## ⚙ Key Features ⚙
### The Calculation Motor: 

A helpful wrapper around yfinance that fetches price history and facilitates the 
 calculation of basic fundamental metrics (like TTM EPS and PE Ratios).

### Persistent Database: 
 
Uses SQLite to store a permanent log of all BUY, SELL, DEPOSIT, and WITHDRAW events.

### Automated Cash Management: 

Automatically deducts cost from your cash balance when you buy and credits cash when 
 you sell.

### Fractional Shares: 

Full support for fractional trading (e.g., buying 0.5 shares).

### Project Structure

motor.py: The "Engine." Contains the CalculationMotor class for fetching OHLC data and computing basic indicators.

db_manager.py: The "Ledger." Handles all SQL connections, records transactions, and updates the portfolio state.

create_db.py: The "Setup." Creates the trades.db schema.

trading.ipynb: Start Here! A Jupyter Notebook containing examples of functions, strategy logic, and backtesting 
 workflows.