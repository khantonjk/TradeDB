import argparse
import pandas as pd
from portfolio_test.motor import CalculationMotor
from portfolio_test.forge_data import DataForge
from portfolio_test.portfolio_manager import PortfolioManager
from portfolio_test.portfolio_service import PortfolioStatisticsService
from portfolio_test.strategies.example_moving_average import EMACrossStrategy
from portfolio_test.strategies.pe_reversion import PEReversionStrategy
from portfolio_test.strategies.pe_short import PEShortStrategy
from portfolio_test.strategies.cash_sweep import CashSweepStrategy
from portfolio_test.strategies.yield_curve_macro import YieldCurveMacroStrategy
from portfolio_test.strategies.credit_spread_macro import CreditSpreadMacroStrategy
from portfolio_test.strategies.move_macro import MoveMacroStrategy
from portfolio_test.strategies.ultimate_macro import UltimateMacroStrategy
from portfolio_test.strategies.fixed_income_arbitrage import FixedIncomeArbitrageStrategy
from portfolio_test.strategies.duration_management import DurationManagementStrategy

def get_strategy(strategy_name: str, ticker: str):
    if strategy_name == "ema_cross":
        return EMACrossStrategy(ticker=ticker)
    elif strategy_name == "pe_reversion":
        return PEReversionStrategy(ticker=ticker, buy_threshold=20.0, sell_threshold=40.0)
    elif strategy_name == "pe_short":
        return PEShortStrategy(ticker=ticker)
    elif strategy_name == "cash_sweep":
        return CashSweepStrategy(ticker=ticker)
    elif strategy_name == "yield_curve_macro":
        return YieldCurveMacroStrategy(ticker=ticker)
    elif strategy_name == "credit_spread_macro":
        return CreditSpreadMacroStrategy(ticker=ticker)
    elif strategy_name == "move_macro":
        return MoveMacroStrategy(ticker=ticker)
    elif strategy_name == "ultimate_macro":
        return UltimateMacroStrategy(ticker=ticker)
    elif strategy_name == "fixed_income_arbitrage":
        return FixedIncomeArbitrageStrategy(ticker=ticker)
    elif strategy_name == "duration_management":
        return DurationManagementStrategy(ticker=ticker)
    # Add future strategies here
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")

def run_backtest(ticker: str, start: str, end: str, strategy_name: str, save_plot: bool, benchmark_ticker: str = "^GSPC", quiet: bool = False):
    def qprint(msg):
        if not quiet:
            print(msg)
            
    qprint(f"--- Starting Backtest for {ticker} using {strategy_name} ---")
    
    # 1. Fetch Market Data
    qprint("Fetching market data...")
    motor = CalculationMotor(ticker, start=start, end=end)
    
    # 2. Forge Data
    qprint("Forging data...")
    forge = DataForge()
    # Add primary price series
    data = forge.add_column_of_data(motor.df["Adj Close"], column_name=f"Close ({ticker})")
    
    # Fetch and forge risk-free rate
    qprint("Fetching Risk-Free Rate (^IRX)...")
    try:
        irx_motor = CalculationMotor("^IRX", start=start, end=end, convert_currency=False)
        irx_data = irx_motor.df["Adj Close"].copy()
        data = forge.add_column_of_data(irx_data, column_name="Risk_Free_Rate")
        data["Risk_Free_Rate"] = data["Risk_Free_Rate"].ffill()
    except Exception as e:
        qprint(f"Warning: Failed to fetch Risk-Free Rate: {e}")
        data["Risk_Free_Rate"] = 0.0

    
    if strategy_name in ("yield_curve_macro", "ultimate_macro"):
        qprint("Fetching 10-Year Treasury Yield (^TNX)...")
        try:
            tnx_motor = CalculationMotor("^TNX", start=start, end=end, convert_currency=False)
            tnx_data = tnx_motor.df["Adj Close"].copy()
            data = forge.add_column_of_data(tnx_data, column_name="10Y_Yield")
            data["10Y_Yield"] = data["10Y_Yield"].ffill()
        except Exception as e:
            qprint(f"Warning: Failed to fetch 10-Year Yield: {e}")
            
    if strategy_name in ("credit_spread_macro", "ultimate_macro"):
        qprint("Fetching High Yield ETF (HYG) and Treasury ETF (IEI)...")
        try:
            hyg_motor = CalculationMotor("HYG", start=start, end=end)
            hyg_data = hyg_motor.df["Adj Close"].copy()
            data = forge.add_column_of_data(hyg_data, column_name="HYG")
            data["HYG"] = data["HYG"].ffill()
            
            iei_motor = CalculationMotor("IEI", start=start, end=end)
            iei_data = iei_motor.df["Adj Close"].copy()
            data = forge.add_column_of_data(iei_data, column_name="IEI")
            data["IEI"] = data["IEI"].ffill()
        except Exception as e:
            qprint(f"Warning: Failed to fetch HYG/IEI: {e}")
            
    if strategy_name == "fixed_income_arbitrage":
        qprint("Fetching Investment Grade (LQD) and Treasury (IEF)...")
        try:
            lqd_motor = CalculationMotor("LQD", start=start, end=end)
            lqd_data = lqd_motor.df["Adj Close"].copy()
            data = forge.add_column_of_data(lqd_data, column_name="LQD")
            data["LQD"] = data["LQD"].ffill()
            
            ief_motor = CalculationMotor("IEF", start=start, end=end)
            ief_data = ief_motor.df["Adj Close"].copy()
            data = forge.add_column_of_data(ief_data, column_name="IEF")
            data["IEF"] = data["IEF"].ffill()
        except Exception as e:
            qprint(f"Warning: Failed to fetch LQD/IEF: {e}")

    if strategy_name in ("move_macro", "ultimate_macro"):
        qprint("Fetching MOVE Index (^MOVE)...")
        try:
            move_motor = CalculationMotor("^MOVE", start=start, end=end, convert_currency=False)
            move_data = move_motor.df["Adj Close"].copy()
            data = forge.add_column_of_data(move_data, column_name="MOVE")
            data["MOVE"] = data["MOVE"].ffill()
        except Exception as e:
            qprint(f"Warning: Failed to fetch MOVE Index: {e}")
            
    if strategy_name in ("pe_reversion", "pe_short"):
        qprint("Fetching and forging PE ratio...")
        pe_ratio = motor.get_daily_pe_ratio()
        data = forge.add_column_of_data(pe_ratio, column_name=f"PE_Ratio ({ticker})")
        data = data.dropna()
    
    # 3. Initialize Strategy & Prepare Data
    qprint("Preparing strategy data...")
    strategy = get_strategy(strategy_name, ticker)
    data = strategy.prepare_data(data)
    
    # 4. Initialize Portfolio Manager
    pm = PortfolioManager(seed_date=start)
    
    # 5. Execute Strategy
    qprint(f"Executing strategy: {strategy.name}...")
    strategy.execute(data, pm)
    
    # 6. Fetch Benchmark Data
    qprint(f"Fetching benchmark data ({benchmark_ticker})...")
    try:
        benchmark_motor = CalculationMotor(benchmark_ticker, start=start, end=end)
        benchmark_data = benchmark_motor.df["Adj Close"].copy()
    except Exception as e:
        qprint(f"Warning: Failed to fetch benchmark data: {e}")
        benchmark_data = None
    
    # 7. Calculate and Print Statistics
    qprint("Calculating statistics...")
    pss = PortfolioStatisticsService(pm, benchmark_series=benchmark_data)
    if not quiet:
        pss.print_performance_summary()
    
    # 8. Generate Plot
    if not quiet:
        if save_plot:
            plot_path = f"backtest_{ticker}_{strategy_name}.png"
            pss.plot_performance(save_path=plot_path)
        else:
            pss.plot_performance()
            
    # Return summary dict
    return pss.get_summary_dict()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Portfolio Backtest")
    parser.add_argument("--ticker", type=str, required=True, help="Ticker symbol (e.g., AAPL)")
    parser.add_argument("--start", type=str, default="2020-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--strategy", type=str, default="ema_cross", help="Strategy name (e.g., ema_cross, pe_reversion)")
    parser.add_argument("--benchmark", type=str, default="^GSPC", help="Benchmark ticker (e.g., ^GSPC, AAPL)")
    parser.add_argument("--save-plot", action="store_true", help="Save plot to file instead of displaying")
    
    args = parser.parse_args()
    run_backtest(args.ticker, args.start, args.end, args.strategy, args.save_plot, args.benchmark)
