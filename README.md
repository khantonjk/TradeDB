# Portfolio Backtester with Transaction Logging
This is a custom-built Python framework designed to backtest trading 
strategies using real-world data. It combines a calculation engine (wrapping the Yahoo Finance API) with an in-memory, DataFrame-based portfolio management system to track portfolio performance and log transactions.

This project bridges the gap between simple data fetching and actual portfolio tracking, 
allowing you to simulate trades, manage a cash balance, and store your trading history temporarily within the application's runtime.

## ⚙ Key Features ⚙
### The Calculation Motor: 

A helpful wrapper around yfinance that fetches price history and facilitates the 
 calculation of basic fundamental metrics (like TTM EPS and PE Ratios).

### In-Memory Portfolio Management: 
 
Uses Pandas DataFrames to store a log of all BUY and SELL events, manage current positions, and track portfolio value history. Data is managed in-memory during runtime.

### Automated Cash Management: 

Automatically deducts cost from your cash balance when you buy and credits cash when 
 you sell.

### Fractional Shares: 

Full support for fractional trading (e.g., buying 0.5 shares).

### Project Structure

motor.py: The "Engine." Contains the CalculationMotor class for fetching OHLC data and computing basic indicators.

forge_data.py: The "Data Forger." Contains the DataForge class for aggregating various data series into a single DataFrame.

portfolio_manager.py: The "Portfolio Manager." Handles all portfolio operations, including recording transactions, updating positions, managing cash, and tracking portfolio value using DataFrames.

trading_strategy_*.ipynb: Jupyter Notebooks containing examples of functions, strategy logic, and backtesting workflows.
