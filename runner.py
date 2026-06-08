import argparse
import pandas as pd
from portfolio_test.motor import CalculationMotor
from portfolio_test.forge_data import DataForge
from portfolio_test.portfolio_manager import PortfolioManager
from portfolio_test.portfolio_service import PortfolioStatisticsService
from portfolio_test.strategies.example_moving_average import EMACrossStrategy
from portfolio_test.strategies.pe_reversion import PEReversionStrategy
from portfolio_test.strategies.pe_short import PEShortStrategy

def get_strategy(strategy_name: str, ticker: str):
    if strategy_name == "ema_cross":
        return EMACrossStrategy(ticker=ticker)
    elif strategy_name == "pe_reversion":
        return PEReversionStrategy(ticker=ticker, buy_threshold=20.0, sell_threshold=40.0)
    elif strategy_name == "pe_short":
        return PEShortStrategy(ticker=ticker)
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
