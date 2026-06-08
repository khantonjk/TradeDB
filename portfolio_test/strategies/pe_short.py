import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager
from portfolio_test.strategies.base_strategy import BaseStrategy

class PEShortStrategy(BaseStrategy):
    """
    A demonstration short-selling strategy.
    
    Rules:
    - If P/E Ratio > 50: We believe the stock is massively overvalued. SHORT 100% of our portfolio equity.
    - If P/E Ratio < 30: The valuation has returned to earth. COVER our entire short position.
    """
    
    def __init__(self, ticker: str = "TSLA", short_threshold: float = 50.0, cover_threshold: float = 30.0):
        self.ticker = ticker
        self.short_threshold = short_threshold
        self.cover_threshold = cover_threshold
        self._name = f"PE Short (>{short_threshold}/<{cover_threshold}) - {ticker}"
        
    @property
    def name(self) -> str:
        return self._name

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        pe_col = f"PE_Ratio ({self.ticker})"
        
        if pe_col not in df.columns:
            raise ValueError(f"Column {pe_col} not found in data. Required for PE Short strategy.")
            
        df['Signal'] = 0
        df.loc[df[pe_col] > self.short_threshold, 'Signal'] = -1  # short signal
        df.loc[df[pe_col] < self.cover_threshold, 'Signal'] = 1   # cover signal
        
        return df

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        is_short = False
        
        for date in data.index:
            pm.update_portfolio_prices(data.loc[date])
            
            signal = data.loc[date, 'Signal']
            
            if signal == -1 and not is_short:
                pm.record_transaction_percentage_buy_sell(
                    tx_type="SHORT", 
                    ticker=self.ticker, 
                    pcnt_of_portfolio=1.0, 
                    data_one_date=data.loc[date]
                )
                is_short = True
            elif signal == 1 and is_short:
                pm.record_transaction_percentage_buy_sell(
                    tx_type="COVER", 
                    ticker=self.ticker, 
                    pcnt_of_portfolio=1.0, 
                    data_one_date=data.loc[date]
                )
                is_short = False
                
        if is_short:
            pm.close_all_positions(tx_datetime=data.index[-1])
