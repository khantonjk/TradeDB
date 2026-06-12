import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager
from portfolio_test.strategies.base_strategy import BaseStrategy

class YieldCurveMacroStrategy(BaseStrategy):
    """
    Macroeconomic strategy that uses the 10-Year vs 3-Month Treasury yield curve to predict recessions.
    
    Rules:
    - Normal Curve (Spread > 0): Buy and hold equities (e.g. SPY).
    - Inverted Curve (Spread < 0): Sell all equities and hide in cash (earning the overnight repo rate).
    """
    
    def __init__(self, ticker: str = "SPY"):
        self.ticker = ticker
        self._name = f"Yield Curve Macro ({ticker})"

    @property
    def name(self) -> str:
        return self._name

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        
        required_cols = ["10Y_Yield", "Risk_Free_Rate"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Column {col} not found in data. Required for Yield Curve Macro strategy.")
                
        # Calculate the raw Yield Spread (10 Year - 3 Month)
        df['Yield_Spread'] = df['10Y_Yield'] - df['Risk_Free_Rate']
        
        # Smooth the spread using a 20-Day Simple Moving Average
        df['Spread_SMA_20'] = df['Yield_Spread'].rolling(window=20, min_periods=1).mean()
        
        # Trend confirmation: 200-Day Moving Average of the stock
        price_col = f"Close ({self.ticker})"
        df['Equity_SMA_200'] = df[price_col].rolling(window=200, min_periods=1).mean()
        
        return df

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        in_position = False
        
        price_col = f"Close ({self.ticker})"
        
        for date in data.index:
            pm.update_portfolio_prices(data.loc[date])
            
            spread = data.loc[date, 'Spread_SMA_20']
            price = data.loc[date, price_col]
            sma_200 = data.loc[date, 'Equity_SMA_200']
            
            # We want to be invested ONLY if the Macro is healthy AND the Trend is healthy
            is_macro_healthy = spread > 0
            is_trend_healthy = price > sma_200
            
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
