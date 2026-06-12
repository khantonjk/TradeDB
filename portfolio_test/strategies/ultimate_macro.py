import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager

class UltimateMacroStrategy:
    """
    The Ultimate Macro Engine.
    Combines Yield Curve, Credit Spreads, and Bond Volatility (MOVE) into a single master risk model.
    If ANY of the macro signals detect panic, the system sells everything and hides in risk-free cash.
    """
    def __init__(self, ticker: str):
        self.name = f"Ultimate Macro Engine ({ticker})"
        self.ticker = ticker

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        
        required_cols = ["10Y_Yield", "Risk_Free_Rate", "HYG", "IEI", "MOVE"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Column {col} not found in data. Required for Ultimate Macro strategy.")
                
        # --- 1. Yield Curve Data ---
        df['Yield_Spread'] = df['10Y_Yield'] - df['Risk_Free_Rate']
        df['Spread_SMA_20'] = df['Yield_Spread'].rolling(window=20, min_periods=1).mean()
        
        # --- 2. Credit Spread Data ---
        df['Credit_Ratio'] = df['HYG'] / df['IEI']
        df['Credit_Ratio_SMA_20'] = df['Credit_Ratio'].rolling(window=20, min_periods=1).mean()
        df['Credit_Ratio_SMA_50'] = df['Credit_Ratio'].rolling(window=50, min_periods=1).mean()
        
        # --- 3. Bond Volatility (MOVE) Data ---
        df['MOVE_SMA_50'] = df['MOVE'].rolling(window=50, min_periods=1).mean()
        df['MOVE_STD_50'] = df['MOVE'].rolling(window=50, min_periods=1).std()
        std_safe = df['MOVE_STD_50'].replace(0, 0.0001)
        df['MOVE_Z_Score'] = (df['MOVE'] - df['MOVE_SMA_50']) / std_safe
        
        # --- 4. Trend Confirmation ---
        price_col = f"Close ({self.ticker})"
        df['Equity_SMA_200'] = df[price_col].rolling(window=200, min_periods=1).mean()
        
        return df

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        in_position = False
        price_col = f"Close ({self.ticker})"
        
        macro_panic = False
        
        for date in data.index:
            pm.update_portfolio_prices(data.loc[date])
            
            # 1. Yield Curve (Panic if Spread < 0)
            # Actually we found earlier that we want to be invested if spread > 0.
            # So Panic = spread < 0
            spread = data.loc[date, 'Spread_SMA_20']
            yield_curve_healthy = spread > 0
            
            # 2. Credit Spreads (Panic if Fast < Slow)
            fast_ratio = data.loc[date, 'Credit_Ratio_SMA_20']
            slow_ratio = data.loc[date, 'Credit_Ratio_SMA_50']
            credit_spread_healthy = fast_ratio > slow_ratio
            
            # 3. Bond Volatility (Panic if Z-Score > 1.5, safe when < 0)
            z_score = data.loc[date, 'MOVE_Z_Score']
            if z_score > 1.5:
                macro_panic = True
            elif z_score < 0:
                macro_panic = False
            # If between 0 and 1.5, maintain previous state
            bond_volatility_healthy = not macro_panic
            
            # Master Signal
            is_macro_healthy = yield_curve_healthy and credit_spread_healthy and bond_volatility_healthy
            
            price = data.loc[date, price_col]
            sma_200 = data.loc[date, 'Equity_SMA_200']
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
