import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager

class CreditSpreadMacroStrategy:
    """
    A Macro Strategy that uses the Credit Spread between High Yield Corporate Bonds (HYG)
    and Treasury Bonds (IEI) to detect financial distress and exit the stock market.
    """
    def __init__(self, ticker: str):
        self.name = f"Credit Spread Macro ({ticker})"
        self.ticker = ticker

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        
        required_cols = ["HYG", "IEI", "Risk_Free_Rate"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Column {col} not found in data. Required for Credit Spread Macro strategy.")
                
        # Calculate the raw Credit Ratio (High Yield Price / Treasury Price)
        # When this drops, High Yield is underperforming Treasuries (Spread is blowing out)
        df['Credit_Ratio'] = df['HYG'] / df['IEI']
        
        # Calculate moving averages for the Credit Ratio
        # A 20-day crossing below a 50-day is a massive warning sign
        df['Credit_Ratio_SMA_20'] = df['Credit_Ratio'].rolling(window=20, min_periods=1).mean()
        df['Credit_Ratio_SMA_50'] = df['Credit_Ratio'].rolling(window=50, min_periods=1).mean()
        
        # Trend confirmation: 200-Day Moving Average of the stock
        price_col = f"Close ({self.ticker})"
        df['Equity_SMA_200'] = df[price_col].rolling(window=200, min_periods=1).mean()
        
        return df

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        in_position = False
        
        price_col = f"Close ({self.ticker})"
        
        for date in data.index:
            pm.update_portfolio_prices(data.loc[date])
            
            fast_ratio = data.loc[date, 'Credit_Ratio_SMA_20']
            slow_ratio = data.loc[date, 'Credit_Ratio_SMA_50']
            
            price = data.loc[date, price_col]
            sma_200 = data.loc[date, 'Equity_SMA_200']
            
            # The Macro is Healthy if the short term ratio is ABOVE the long term ratio
            # (Meaning High Yield is currently outperforming or keeping pace with Treasuries)
            is_macro_healthy = fast_ratio > slow_ratio
            
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
