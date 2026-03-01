from datetime import datetime
import pandas as pd
from portfolio_test.forge_data import DataForge
#from db_comm import PortfolioDBManager, DATABASE_NAME
from portfolio_test.portfolio_manager import PortfolioManager

#pm = PortfolioDBManager(DATABASE_NAME)
pm = PortfolioManager()

from portfolio_test.motor import CalculationMotor

data_forge = DataForge()

cm_temp = CalculationMotor("AAPL")

import numpy as np

# if data exists
if "data" not in locals():
    data = data_forge.add_column_of_data(cm_temp.df["Adj Close"], column_name="Close (AAPL)")


pm.record_transaction_percentage_buy_sell(
    tx_type="BUY",
    ticker="AAPL",
    pcnt_of_portfolio=1,  # buy with 10% of portfolio value
    data_one_date=data.loc["2025-01-03"]
)

assert(pm.get_cash_balance() == 0)

pm.record_transaction_percentage_buy_sell(
    tx_type="SELL",
    ticker="AAPL",
    pcnt_of_portfolio=1,
    data_one_date=data.loc["2025-01-06"]
)

cash = pm.get_cash_balance()
assert cash > 0, "Cash balance should be positive after selling the stock."

hist = pm.get_trades_history()
port_ss = pm.get_portfolio_snapshot()

print('Hist:' , hist)
assert len(hist) > 0, "History is empty."

print('Portfolio Snapshot:' , port_ss)
# assert Portfolio snapshot size = 1 row
assert len(port_ss) == 1, "Portfolio snapshot should have 1 row after buying and selling the stock."
# assert that row is CASH
assert any(port_ss.loc["CASH"])