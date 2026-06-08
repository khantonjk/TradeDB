import sys
import io

# Optional: Force UTF-8 encoding for standard output so emojis print correctly on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from runner import run_backtest

if __name__ == "__main__":
    # You can easily change these variables to test different scenarios
    TICKER = "TSLA"
    START_DATE = "2021-01-01"
    END_DATE = "2024-01-01"
    STRATEGY = "pe_short"
    BENCHMARK = "^GSPC"
    SAVE_PLOT = False  # Set to True if you want to save the image instead of displaying it

    print(f"Executing simple run script for {TICKER}...")
    
    # Run the backtest
    run_backtest(
        ticker=TICKER,
        start=START_DATE,
        end=END_DATE,
        strategy_name=STRATEGY,
        save_plot=SAVE_PLOT,
        benchmark_ticker=BENCHMARK
    )
