import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager
from portfolio_test.strategies.base_strategy import BaseStrategy

class EMACrossStrategy(BaseStrategy):
    """
    Exponential Moving Average (EMA) Crossover Strategy.
    Buys when the fast EMA crosses above the slow EMA.
    Sells when the fast EMA crosses below the slow EMA.
    """
    
    def __init__(self, ticker: str, fast_period: int = 20, slow_period: int = 50):
        self.ticker = ticker
        self.fast_period = fast_period
        self.slow_period = slow_period
        self._name = f"EMA Cross ({fast_period}/{slow_period}) - {ticker}"
        
    @property
    def name(self) -> str:
        return self._name

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        
        # We assume the price column is named "Close ({ticker})"
        price_col = f"Close ({self.ticker})"
        
        if price_col not in df.columns:
            # Fallback if there is only one column and we just want to test
            raise ValueError(f"Column {price_col} not found in data. Required for EMA cross. Columns are: {list(df.columns)}")
            
        df['EMA_fast'] = df[price_col].ewm(span=self.fast_period).mean()
        df['EMA_slow'] = df[price_col].ewm(span=self.slow_period).mean()
        
        # Generate Signals: 1 if fast > slow, 0 otherwise
        df['Signal'] = (df['EMA_fast'] > df['EMA_slow']).astype(int)
        return df

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        for date in data.index:
            # 1. Always update portfolio prices daily so we track daily performance!
            pm.update_portfolio_prices(data.loc[date])
            
            signal = data.loc[date, 'Signal']
            
            # Need to get previous signal to determine crossing
            idx = data.index.get_loc(date)
            if idx > 0:
                prev_date = data.index[idx - 1]
                signal_prev = data.loc[prev_date, 'Signal']
            else:
                signal_prev = 0

            if signal == 1 and signal_prev == 0:
                # Fast crossed ABOVE slow -> BUY
                pm.record_transaction_percentage_buy_sell(
                    tx_type="BUY", 
                    ticker=self.ticker, 
                    pcnt_of_portfolio=1.0, # Buy with 100% of available cash
                    data_one_date=data.loc[date]
                )
            elif signal == 0 and signal_prev == 1:
                # Fast crossed BELOW slow -> SELL
                pm.record_transaction_percentage_buy_sell(
                    tx_type="SELL", 
                    ticker=self.ticker, 
                    pcnt_of_portfolio=1.0, # Sell 100% of position
                    data_one_date=data.loc[date]
                )
