import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager
from portfolio_test.strategies.base_strategy import BaseStrategy

class PEReversionStrategy(BaseStrategy):
    """
    P/E Reversion Strategy.
    Buys the stock when the P/E ratio drops below the `buy_threshold`.
    Sells the stock when the P/E ratio rises above the `sell_threshold`.
    """
    
    def __init__(self, ticker: str, buy_threshold: float = 20.0, sell_threshold: float = 30.0):
        self.ticker = ticker
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self._name = f"PE Reversion (<{buy_threshold}/>{sell_threshold}) - {ticker}"
        
    @property
    def name(self) -> str:
        return self._name

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        
        pe_col = f"PE_Ratio ({self.ticker})"
        
        if pe_col not in df.columns:
            raise ValueError(f"Column {pe_col} not found in data. Required for PE Reversion strategy.")
            
        # Signal: 1 for BUY zone, -1 for SELL zone, 0 for HOLD zone
        df['Signal'] = 0
        df.loc[df[pe_col] < self.buy_threshold, 'Signal'] = 1
        df.loc[df[pe_col] > self.sell_threshold, 'Signal'] = -1
        
        return df

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        in_position = False
        
        for date in data.index:
            # 1. Update prices daily for accurate mark-to-market performance
            pm.update_portfolio_prices(data.loc[date])
            
            signal = data.loc[date, 'Signal']
            
            if signal == 1 and not in_position:
                # PE dropped below 20, buy!
                pm.record_transaction_percentage_buy_sell(
                    tx_type="BUY", 
                    ticker=self.ticker, 
                    pcnt_of_portfolio=1.0, 
                    data_one_date=data.loc[date]
                )
                in_position = True
            elif signal == -1 and in_position:
                # PE rose above 30, sell!
                pm.record_transaction_percentage_buy_sell(
                    tx_type="SELL", 
                    ticker=self.ticker, 
                    pcnt_of_portfolio=1.0, 
                    data_one_date=data.loc[date]
                )
                in_position = False
