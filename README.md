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

- **`motor.py` (The Calculation Motor)**: The "Engine" wrapper around yfinance that fetches OHLC price history and computes fundamental metrics like TTM EPS and P/E Ratios.
- **`forge_data.py` (The Data Forger)**: Responsible for aggregating various data series into a unified DataFrame.
- **`portfolio_manager.py` (The Portfolio Manager)**: Manages the in-memory portfolio using DataFrames. It handles recording buy/sell transactions, supporting fractional shares, maintaining current positions, and automatically adjusting your cash balance.
- **`portfolio_service.py` (The Portfolio Statistics Service)**: A professional-grade statistics engine that evaluates your strategy's performance against a benchmark (e.g., S&P 500). It calculates crucial risk and performance metrics such as Risk-Adjusted Returns (Sharpe Ratio), Risk Metrics (Volatility, Max Drawdown), Trade Statistics (Win Rate), and Absolute Returns.
- **`trading_strategy_*.ipynb` (Jupyter Notebooks)**: Workspaces where you design and backtest your strategies, leveraging the Calculation Motor for data, the Portfolio Manager to simulate trades, and the Statistics Service to generate comprehensive performance reports comparing your strategy to market benchmarks.

// Signed off by Anton