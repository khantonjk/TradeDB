# yfinance Data Guide

The `yfinance` library provides a robust API for retrieving financial data from Yahoo Finance. This guide details the various data points you can extract to feed into our `CalculationMotor` and `DataForge`.

## 1. Price History (OHLCV)
This is the most common use case and what our `CalculationMotor` currently handles.
```python
import yfinance as yf
ticker = yf.Ticker("AAPL")

# Fetch historical data (Open, High, Low, Close, Volume, Dividends, Stock Splits)
hist = ticker.history(period="1mo") # "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"
```

## 2. Fundamental & Metadata (`ticker.info`)
The `.info` dictionary contains a massive amount of fundamental metadata.
```python
info = ticker.info
```
**Key points available in `.info`:**
- **Valuation Measures**: `trailingPE`, `forwardPE`, `priceToBook`, `enterpriseToEbitda`, `pegRatio`, `marketCap`, `enterpriseValue`
- **Financial Highlights**: `profitMargins`, `operatingMargins`, `returnOnAssets`, `returnOnEquity`, `revenueGrowth`, `earningsGrowth`
- **Dividends & Yields**: `dividendRate`, `dividendYield`, `fiveYearAvgDividendYield`, `payoutRatio`
- **Trading Info**: `beta`, `52WeekChange`, `fiftyTwoWeekLow`, `fiftyTwoWeekHigh`, `fiftyDayAverage`, `twoHundredDayAverage`, `averageVolume`
- **Company Info**: `sector`, `industry`, `fullTimeEmployees`, `longBusinessSummary`

## 3. Financial Statements
Access quarterly or annual financial statements.
```python
# Income Statement
income = ticker.financials
q_income = ticker.quarterly_financials

# Balance Sheet
balance = ticker.balance_sheet
q_balance = ticker.quarterly_balance_sheet

# Cash Flow
cashflow = ticker.cashflow
q_cashflow = ticker.quarterly_cashflow
```

## 4. Corporate Actions
Get historical dividends and stock splits.
```python
actions = ticker.actions
dividends = ticker.dividends
splits = ticker.splits
```

## 5. Options Chain
Fetch available options data for a specific expiration date.
```python
# Get available expiration dates
expirations = ticker.options

# Get calls/puts for a specific date
opt = ticker.option_chain(expirations[0])
calls = opt.calls
puts = opt.puts
```

## 6. Institutional Holders & Short Interest
See who owns the stock and how heavily it's shorted.
```python
holders = ticker.institutional_holders
mutual_funds = ticker.mutualfund_holders
major_holders = ticker.major_holders

# Shares Short (also available in ticker.info['sharesShort'])
```

## 7. Earnings
Historical and upcoming earnings estimates.
```python
earnings_dates = ticker.earnings_dates
```

---

> [!TIP]
> **How to use this with DataForge:**
> If you want to use fundamental data (like PE ratio or market cap) in your backtesting, remember that `.info` only provides the *current* real-time value. To get historical fundamental metrics, we must calculate them manually inside `motor.py` using historical price data and historical financial statements.
