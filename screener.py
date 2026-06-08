import pandas as pd
import matplotlib.pyplot as plt
from runner import run_backtest

# Top 15 popular stocks
DEFAULT_STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", 
    "META", "TSLA", "NFLX", "AMD", "INTC", 
    "CSCO", "PEP", "KO", "JNJ", "V"
]

import os
import contextlib

@contextlib.contextmanager
def suppress_stdout():
    with open(os.devnull, 'w', encoding='utf-8') as fnull:
        with contextlib.redirect_stdout(fnull):
            yield

def run_screener(stocks=DEFAULT_STOCKS, start="2021-01-01", end="2024-01-01", strategy="pe_reversion", benchmark="^GSPC", save_plot=False):
    print(f"🚀 Starting Multi-Stock Screener across {len(stocks)} stocks...")
    print(f"Strategy: {strategy} | Period: {start} to {end}")
    print("-" * 60)
    
    results = []
    
    for idx, ticker in enumerate(stocks):
        print(f"[{idx+1}/{len(stocks)}] Testing {ticker}...", end=" ", flush=True)
        try:
            with suppress_stdout():
                summary = run_backtest(
                    ticker=ticker,
                    start=start,
                    end=end,
                    strategy_name=strategy,
                    save_plot=False,
                    benchmark_ticker=benchmark,
                    quiet=True
                )
            summary['Ticker'] = ticker
            results.append(summary)
            print("✅ Done")
        except Exception as e:
            print(f"❌ Failed ({str(e)})")
            
    if not results:
        print("No successful backtests completed.")
        return
        
    df = pd.DataFrame(results)
    
    # Reorder columns to put Ticker first
    cols = ['Ticker'] + [c for c in df.columns if c != 'Ticker']
    df = df[cols]
    
    # Sort by Sharpe Ratio
    df = df.sort_values(by='Sharpe Ratio', ascending=False).reset_index(drop=True)
    
    print("\n" + "=" * 80)
    print(f"🏆 SCREENER RESULTS (Ranked by Sharpe Ratio)")
    print("=" * 80)
    
    # Format the dataframe for display
    display_df = df.copy()
    for col in ['Total Return', 'Annualized Return', 'Volatility', 'Max Drawdown', 'Win Rate']:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"{x*100:.2f}%" if pd.notnull(x) else "N/A")
            
    for col in ['Sharpe Ratio']:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else "N/A")
            
    print(display_df[['Ticker', 'Total Return', 'Sharpe Ratio', 'Max Drawdown', 'Total Trades', 'Win Rate']].to_string(index=False))
    
    print("\n" + "=" * 80)
    print(f"📊 AGGREGATE PERFORMANCE SUMMARY")
    print("=" * 80)
    
    # Calculate Worst, Best, Median, Average for Total Return
    returns = df['Total Return'].dropna()
    if not returns.empty:
        worst = returns.min() * 100
        best = returns.max() * 100
        median = returns.median() * 100
        avg = returns.mean() * 100
        
        worst_ticker = df.loc[df['Total Return'] == returns.min(), 'Ticker'].values[0]
        best_ticker = df.loc[df['Total Return'] == returns.max(), 'Ticker'].values[0]
        
        print(f"Worst Performance:   {worst:>8.2f}% ({worst_ticker})")
        print(f"Median Performance:  {median:>8.2f}%")
        print(f"Average Performance: {avg:>8.2f}%")
        print(f"Best Performance:    {best:>8.2f}% ({best_ticker})")
    
    # Save to CSV
    df.to_csv("screener_results.csv", index=False)
    print("\n💾 Full detailed results saved to 'screener_results.csv'")
    
    # Plot results
    plt.figure(figsize=(12, 6))
    
    # Sort dataframe by Total Return for a better looking chart
    chart_df = df.sort_values(by='Total Return', ascending=False)
    
    bars = plt.bar(chart_df['Ticker'], chart_df['Total Return'] * 100, 
                   color=['green' if x > 0 else 'red' if x < 0 else 'gray' for x in chart_df['Total Return']])
    
    plt.axhline(0, color='black', linewidth=1)
    plt.title(f"Screener Results: {strategy} ({start} to {end})")
    plt.ylabel("Total Return (%)")
    plt.xlabel("Ticker")
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Add values on top of bars
    for bar in bars:
        height = bar.get_height()
        if height != 0:
            plt.text(bar.get_x() + bar.get_width()/2., height + (1 if height > 0 else -3),
                     f'{height:.1f}%',
                     ha='center', va='bottom' if height > 0 else 'top', fontsize=8)

    plt.tight_layout()
    if save_plot:
        plt.savefig("screener_results.png")
        print("📊 Plot saved to 'screener_results.png'")
    else:
        plt.show()

if __name__ == "__main__":
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    run_screener()
