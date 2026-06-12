import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager

class MoveMacroStrategy:
    """
    A Macro Strategy that uses the ICE BofA MOVE Index (^MOVE) to detect bond market panic.
    If bond market volatility spikes, the strategy exits the stock market.
    """
    def __init__(self, ticker: str):
        self.name = f"MOVE Volatility Macro ({ticker})"
        self.ticker = ticker

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        
        required_cols = ["MOVE", "Risk_Free_Rate"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Column {col} not found in data. Required for MOVE Macro strategy.")
                
        # Calculate the 50-day SMA and STD for the MOVE index
        df['MOVE_SMA_50'] = df['MOVE'].rolling(window=50, min_periods=1).mean()
        df['MOVE_STD_50'] = df['MOVE'].rolling(window=50, min_periods=1).std()
        
        # Calculate the Z-Score. Avoid division by zero by replacing 0 with a small number.
        std_safe = df['MOVE_STD_50'].replace(0, 0.0001)
        df['MOVE_Z_Score'] = (df['MOVE'] - df['MOVE_SMA_50']) / std_safe
        
        # Trend confirmation: 200-Day Moving Average of the stock
        price_col = f"Close ({self.ticker})"
        df['Equity_SMA_200'] = df[price_col].rolling(window=200, min_periods=1).mean()
        
        return df

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        in_position = False
        price_col = f"Close ({self.ticker})"
        
        # Wait until we have enough data for a meaningful standard deviation
        # (Though min_periods=1 handles early dates, Z-scores are wild with < 10 days of data)
        # We'll just run through all dates, but the first few days might have wild Z-scores.
        
        for date in data.index:
            pm.update_portfolio_prices(data.loc[date])
            
            z_score = data.loc[date, 'MOVE_Z_Score']
            
            price = data.loc[date, price_col]
            sma_200 = data.loc[date, 'Equity_SMA_200']
            
            # The Macro is Healthy if the bond market is NOT panicking.
            # We define panic as Z-Score > 1.5. 
            # Once it panics, we stay out until Z-Score drops below 0 (mean reversion).
            if z_score > 1.5:
                macro_panic = True
            elif z_score < 0:
                macro_panic = False
            else:
                # If it's between 0 and 1.5, we maintain the previous state (hysteresis).
                # To do this cleanly, we can look at whether we are currently in_position.
                # If we are in position, we haven't panicked yet.
                # If we are out of position, we are still waiting for it to drop below 0.
                macro_panic = not in_position

            is_macro_healthy = not macro_panic
            
            # The Trend is Healthy if Price > 200-Day SMA
            is_trend_healthy = price > sma_200
            
            # We only want to hold stocks if BOTH conditions are met!
            should_be_invested = is_macro_healthy and is_trend_healthy
            
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
