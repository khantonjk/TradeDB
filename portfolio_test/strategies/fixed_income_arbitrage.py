import pandas as pd
from portfolio_test.portfolio_manager import PortfolioManager

class FixedIncomeArbitrageStrategy:
    """
    Fixed Income Statistical Arbitrage (Pairs Trading).
    Goes Long LQD and Short IEF when the spread blows out, and reverts when it normalizes.
    """
    def __init__(self, ticker: str):
        self.name = "Fixed Income Arbitrage (LQD vs IEF)"
        self.ticker = ticker  # Not used directly for trading, just for compatibility

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        
        required_cols = ["LQD", "IEF"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Column {col} not found in data. Required for Fixed Income Arbitrage strategy.")
                
        # Calculate the Spread Ratio
        df['Ratio'] = df['LQD'] / df['IEF']
        
        # Calculate rolling 20-day Mean and Standard Deviation
        df['Ratio_Mean_20'] = df['Ratio'].rolling(window=20, min_periods=1).mean()
        df['Ratio_STD_20'] = df['Ratio'].rolling(window=20, min_periods=1).std()
        
        # Calculate Z-Score
        std_safe = df['Ratio_STD_20'].replace(0, 0.0001)
        df['Z_Score'] = (df['Ratio'] - df['Ratio_Mean_20']) / std_safe
        
        return df

    def execute(self, data: pd.DataFrame, pm: PortfolioManager):
        current_state = 0  # 0 = Flat, 1 = Long LQD/Short IEF, -1 = Short LQD/Long IEF
        
        for date in data.index:
            pm.update_portfolio_prices(data.loc[date])
            
            z_score = data.loc[date, 'Z_Score']
            
            # Entry logic
            if current_state == 0:
                if z_score < -2.0:
                    # LQD is too cheap compared to IEF. Buy LQD, Short IEF.
                    pm.record_transaction_percentage_buy_sell(tx_type="BUY", ticker="LQD", pcnt_of_portfolio=0.5, data_one_date=data.loc[date])
                    pm.record_transaction_percentage_buy_sell(tx_type="SHORT", ticker="IEF", pcnt_of_portfolio=0.5, data_one_date=data.loc[date])
                    current_state = 1
                elif z_score > 2.0:
                    # LQD is too expensive compared to IEF. Short LQD, Buy IEF.
                    pm.record_transaction_percentage_buy_sell(tx_type="SHORT", ticker="LQD", pcnt_of_portfolio=0.5, data_one_date=data.loc[date])
                    pm.record_transaction_percentage_buy_sell(tx_type="BUY", ticker="IEF", pcnt_of_portfolio=0.5, data_one_date=data.loc[date])
                    current_state = -1
            
            # Exit logic (Mean Reversion)
            else:
                # If we are Long LQD/Short IEF, we wait for Z-Score to cross back above 0
                if current_state == 1 and z_score >= 0:
                    pm.close_all_positions(tx_datetime=date)
                    current_state = 0
                # If we are Short LQD/Long IEF, we wait for Z-Score to cross back below 0
                elif current_state == -1 and z_score <= 0:
                    pm.close_all_positions(tx_datetime=date)
                    current_state = 0
                    
        # Final cleanup
        if current_state != 0:
            pm.close_all_positions(tx_datetime=data.index[-1])
