import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager

class DurationManagementStrategy:
    """
    Duration Management (Trend Following Bonds).
    Uses a 100-Day Moving Average to determine if Long-Term Bonds (TLT) are in an uptrend.
    If the price drops below the 100-Day SMA, it exits to cash (which earns the overnight Risk-Free Repo rate).
    """
    def __init__(self, ticker: str):
        self.name = f"Duration Management ({ticker})"
        self.ticker = ticker

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        
        price_col = f"Close ({self.ticker})"
        if price_col not in df.columns:
            # Fallback to the ticker itself (in case of custom injection)
            if self.ticker in df.columns:
                price_col = self.ticker
            else:
                raise ValueError(f"Column {price_col} not found in data.")
                
        # Calculate the 100-Day Simple Moving Average
        df['SMA_100'] = df[price_col].rolling(window=100, min_periods=1).mean()
        
        return df

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        in_position = False
        
        price_col = f"Close ({self.ticker})"
        if price_col not in data.columns:
            price_col = self.ticker
            
        for date in data.index:
            pm.update_portfolio_prices(data.loc[date])
            
            price = data.loc[date, price_col]
            sma_100 = data.loc[date, 'SMA_100']
            
            # The Trend logic: 
            # If price > 100-Day SMA, we want to be fully invested in Long Duration bonds.
            # If price < 100-Day SMA, we want 0 duration (Cash / T-Bills).
            should_be_invested = price > sma_100
            
            if should_be_invested and not in_position:
                pm.record_transaction_percentage_buy_sell(
                    tx_type="BUY", 
                    ticker=self.ticker, 
                    pcnt_of_portfolio=1.0, 
                    data_one_date=data.loc[date]
                )
                in_position = True
                
            elif not should_be_invested and in_position:
                pm.record_transaction_percentage_buy_sell(
                    tx_type="SELL", 
                    ticker=self.ticker, 
                    pcnt_of_portfolio=1.0, 
                    data_one_date=data.loc[date]
                )
                in_position = False
                        
        # Close all positions at the end to finalize PnL
        if in_position:
            pm.close_all_positions(tx_datetime=data.index[-1])
