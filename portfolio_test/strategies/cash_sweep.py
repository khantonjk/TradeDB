import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager
from portfolio_test.strategies.base_strategy import BaseStrategy

class CashSweepStrategy(BaseStrategy):
    """
    A strategy that does absolutely nothing.
    It just sits in 100% Cash, allowing the PortfolioManager's Overnight Repo sweep
    to accrue daily risk-free interest.
    """
    
    def __init__(self, ticker: str = "AAPL"):
        self.ticker = ticker
        self._name = "100% Cash (Repo Sweep)"
        
    @property
    def name(self) -> str:
        return self._name

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        return data

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        # We don't execute any buys or sells!
        # We just update prices daily, which triggers the Overnight Repo sweep in the PortfolioManager
        for date in data.index:
            pm.update_portfolio_prices(data.loc[date])
