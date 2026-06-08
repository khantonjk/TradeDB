import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class PortfolioStatisticsService:
    def __init__(self, portfolio_manager, benchmark_series: pd.Series = None):
        self.pm = portfolio_manager
        self.benchmark_series = self._validate_benchmark(benchmark_series)
        self.portfolio_value_history = self.pm.get_portfolio_value_history()
        self.trades_history = self.pm.get_trades_history()

    def _validate_benchmark(self, benchmark_series):
        if benchmark_series is None:
            return None
        series = benchmark_series.copy()
        series.index = pd.to_datetime(series.index, errors='raise')
        series.index = pd.DatetimeIndex(series.index).normalize()
        series = series.sort_index(ascending=True)
        return series

    def get_total_valuation(self) -> float:
        return self.pm.get_current_portfolio_value()

    def get_portfolio_returns(self) -> pd.Series:
        portfolio_values = self.portfolio_value_history['total_value'].copy()
        daily_returns = portfolio_values.pct_change().dropna()
        return daily_returns

    def get_benchmark_returns(self) -> pd.Series:
        if self.benchmark_series is None:
            raise ValueError("Benchmark series not provided.")
        daily_returns = self.benchmark_series.pct_change().dropna()
        return daily_returns

    def get_sharpe_ratio(self, risk_free_rate: float = 0.02) -> dict:
        daily_returns = self.get_portfolio_returns()

        if daily_returns.empty:
            raise ValueError("No portfolio returns data available.")

        avg_daily_return = daily_returns.mean()
        std_daily_return = daily_returns.std()

        trading_days = 252
        annualized_return = avg_daily_return * trading_days
        annualized_volatility = std_daily_return * (trading_days ** 0.5)

        if annualized_volatility == 0:
            sharpe_ratio = 0
        else:
            sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility

        return {
            'annualized_return': annualized_return,
            'annualized_volatility': annualized_volatility,
            'sharpe_ratio': sharpe_ratio
        }

    def get_benchmark_sharpe_ratio(self, risk_free_rate: float = 0.02) -> dict:
        if self.benchmark_series is None:
            raise ValueError("Benchmark series not provided.")

        daily_returns = self.get_benchmark_returns()

        if daily_returns.empty:
            raise ValueError("No benchmark returns data available.")

        avg_daily_return = daily_returns.mean()
        std_daily_return = daily_returns.std()

        trading_days = 252
        annualized_return = avg_daily_return * trading_days
        annualized_volatility = std_daily_return * (trading_days ** 0.5)

        if annualized_volatility == 0:
            sharpe_ratio = 0
        else:
            sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility

        return {
            'annualized_return': annualized_return,
            'annualized_volatility': annualized_volatility,
            'sharpe_ratio': sharpe_ratio
        }

    def get_total_return(self) -> float:
        starting_value = self.pm.starting_cash
        ending_value = self.get_total_valuation()
        return (ending_value - starting_value) / starting_value

    def get_benchmark_total_return(self) -> float:
        if self.benchmark_series is None:
            raise ValueError("Benchmark series not provided.")

        start_price = self.benchmark_series.iloc[0]
        end_price = self.benchmark_series.iloc[-1]
        return (end_price - start_price) / start_price

    def get_max_drawdown(self) -> dict:
        portfolio_values = self.portfolio_value_history['total_value'].copy()
        running_max = portfolio_values.expanding().max()
        drawdown = (portfolio_values - running_max) / running_max

        max_drawdown = drawdown.min()
        max_drawdown_date = drawdown.idxmin()

        return {
            'max_drawdown': max_drawdown,
            'max_drawdown_date': max_drawdown_date,
            'drawdown_series': drawdown
        }

    def get_benchmark_max_drawdown(self) -> dict:
        if self.benchmark_series is None:
            raise ValueError("Benchmark series not provided.")

        benchmark_values = self.benchmark_series.copy()
        running_max = benchmark_values.expanding().max()
        drawdown = (benchmark_values - running_max) / running_max

        max_drawdown = drawdown.min()
        max_drawdown_date = drawdown.idxmin()

        return {
            'max_drawdown': max_drawdown,
            'max_drawdown_date': max_drawdown_date,
            'drawdown_series': drawdown
        }

    def get_win_rate(self) -> dict:
        """
        Calculates the win rate of the portfolio based on completed trades.
        Handles both LONG (BUY -> SELL) and SHORT (SHORT -> COVER) trades.
        """
        winning_trades = 0
        losing_trades = 0

        # Calculate Long Trades
        long_trades = self.trades_history[self.trades_history['transaction_type'].isin(['BUY', 'SELL'])].copy()
        if len(long_trades) > 0:
            buy_trades = long_trades[long_trades['transaction_type'] == 'BUY']
            sell_trades = long_trades[long_trades['transaction_type'] == 'SELL']
            for ticker in buy_trades['ticker'].unique():
                buy_price = buy_trades[buy_trades['ticker'] == ticker]['price'].values
                sell_price = sell_trades[sell_trades['ticker'] == ticker]['price'].values
                for b, s in zip(buy_price, sell_price):
                    if s > b:
                        winning_trades += 1
                    else:
                        losing_trades += 1

        # Calculate Short Trades
        short_trades = self.trades_history[self.trades_history['transaction_type'].isin(['SHORT', 'COVER'])].copy()
        if len(short_trades) > 0:
            short_entry = short_trades[short_trades['transaction_type'] == 'SHORT']
            cover_exit = short_trades[short_trades['transaction_type'] == 'COVER']
            for ticker in short_entry['ticker'].unique():
                entry_price = short_entry[short_entry['ticker'] == ticker]['price'].values
                exit_price = cover_exit[cover_exit['ticker'] == ticker]['price'].values
                for e, c in zip(entry_price, exit_price):
                    if c < e: # For shorts, lower exit price is a win
                        winning_trades += 1
                    else:
                        losing_trades += 1

        total_trades = winning_trades + losing_trades
        win_rate = winning_trades / total_trades if total_trades > 0 else 0

        return {
            'win_rate': win_rate,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'total_trades': total_trades
        }

    def compare_to_benchmark(self) -> dict:
        if self.benchmark_series is None:
            raise ValueError("Benchmark series not provided.")

        portfolio_return = self.get_total_return()
        benchmark_return = self.get_benchmark_total_return()

        portfolio_sharpe = self.get_sharpe_ratio()
        benchmark_sharpe = self.get_benchmark_sharpe_ratio()

        portfolio_drawdown = self.get_max_drawdown()
        benchmark_drawdown = self.get_benchmark_max_drawdown()

        excess_return = portfolio_return - benchmark_return

        return {
            'portfolio_return': portfolio_return,
            'benchmark_return': benchmark_return,
            'excess_return': excess_return,
            'portfolio_sharpe': portfolio_sharpe['sharpe_ratio'],
            'benchmark_sharpe': benchmark_sharpe['sharpe_ratio'],
            'sharpe_outperformance': portfolio_sharpe['sharpe_ratio'] - benchmark_sharpe['sharpe_ratio'],
            'portfolio_max_drawdown': portfolio_drawdown['max_drawdown'],
            'benchmark_max_drawdown': benchmark_drawdown['max_drawdown'],
            'drawdown_difference': portfolio_drawdown['max_drawdown'] - benchmark_drawdown['max_drawdown'],
        }

    def print_performance_summary(self):
        print("\n" + "=" * 90)
        print("PORTFOLIO PERFORMANCE SUMMARY".center(90))
        print("=" * 90)

        portfolio_return = self.get_total_return()
        portfolio_stats = self.get_sharpe_ratio()
        portfolio_drawdown = self.get_max_drawdown()
        starting_value = self.pm.starting_cash
        current_value = self.get_total_valuation()
        absolute_gain = current_value - starting_value

        print(f"\n📊 PORTFOLIO METRICS:")
        print(f"   ├─ Starting Capital:        {starting_value:>15,.2f} SEK")
        print(f"   ├─ Current Value:           {current_value:>15,.2f} SEK")
        print(f"   ├─ Absolute Gain/Loss:      {absolute_gain:>15,.2f} SEK")
        print(f"   ├─ Total Return:            {portfolio_return*100:>14.2f}%")
        print(f"   ├─ Annualized Return:       {portfolio_stats['annualized_return']*100:>14.2f}%")
        print(f"   ├─ Annualized Volatility:   {portfolio_stats['annualized_volatility']*100:>14.2f}%")
        print(f"   ├─ Sharpe Ratio:            {portfolio_stats['sharpe_ratio']:>15.4f}")
        print(f"   └─ Max Drawdown:            {portfolio_drawdown['max_drawdown']*100:>14.2f}%")

        win_rate_stats = self.get_win_rate()
        print(f"\n📈 TRADE STATISTICS:")
        print(f"   ├─ Total Trades:            {win_rate_stats['total_trades']:>15d}")
        print(f"   ├─ Winning Trades:          {win_rate_stats['winning_trades']:>15d}")
        print(f"   ├─ Losing Trades:           {win_rate_stats['losing_trades']:>15d}")
        print(f"   └─ Win Rate:                {win_rate_stats['win_rate']*100:>14.2f}%")

        if self.benchmark_series is not None:
            comparison = self.compare_to_benchmark()
            print(f"\n📍 BENCHMARK COMPARISON:")
            print(f"   ├─ Portfolio Return:        {comparison['portfolio_return']*100:>14.2f}%")
            print(f"   ├─ Benchmark Return:        {comparison['benchmark_return']*100:>14.2f}%")
            print(f"   ├─ Excess Return:           {comparison['excess_return']*100:>14.2f}%")
            print(f"   ├─ Portfolio Sharpe:        {comparison['portfolio_sharpe']:>15.4f}")
            print(f"   ├─ Benchmark Sharpe:        {comparison['benchmark_sharpe']:>15.4f}")
            print(f"   ├─ Sharpe Outperformance:   {comparison['sharpe_outperformance']:>15.4f}")
            print(f"   ├─ Portfolio Max DD:        {comparison['portfolio_max_drawdown']*100:>14.2f}%")
            print(f"   └─ Benchmark Max DD:        {comparison['benchmark_max_drawdown']*100:>14.2f}%")

            print(f"\n🎯 VERDICT:")
            if comparison['excess_return'] > 0:
                outperformance = comparison['excess_return'] * 100
                sharpe_out = comparison['sharpe_outperformance']
                print(f"   ✅ Strategy OUTPERFORMED the benchmark")
                print(f"      └─ Excess Return: +{outperformance:.2f}%")
                print(f"      └─ Sharpe Outperformance: +{sharpe_out:.4f}")
            else:
                underperformance = abs(comparison['excess_return']) * 100
                sharpe_out = comparison['sharpe_outperformance']
                print(f"   ❌ Strategy UNDERPERFORMED the benchmark")
                print(f"      └─ Shortfall: -{underperformance:.2f}%")
                print(f"      └─ Sharpe Underperformance: {sharpe_out:.4f}")
        else:
            print(f"\n💡 NOTE: No benchmark provided. To enable benchmark comparison, pass a benchmark_series.")

        print("\n" + "=" * 90 + "\n")

    def get_summary_dict(self) -> dict:
        """Returns a dictionary of key performance metrics for easy consumption."""
        portfolio_return = self.get_total_return()
        
        try:
            portfolio_stats = self.get_sharpe_ratio()
            annualized_return = portfolio_stats['annualized_return']
            annualized_volatility = portfolio_stats['annualized_volatility']
            sharpe_ratio = portfolio_stats['sharpe_ratio']
        except Exception:
            annualized_return = 0
            annualized_volatility = 0
            sharpe_ratio = 0
            
        try:
            portfolio_drawdown = self.get_max_drawdown()['max_drawdown']
        except Exception:
            portfolio_drawdown = 0

        win_rate_stats = self.get_win_rate()

        summary = {
            'Total Return': portfolio_return,
            'Annualized Return': annualized_return,
            'Volatility': annualized_volatility,
            'Sharpe Ratio': sharpe_ratio,
            'Max Drawdown': portfolio_drawdown,
            'Total Trades': win_rate_stats['total_trades'],
            'Win Rate': win_rate_stats['win_rate']
        }
        
        if self.benchmark_series is not None:
            try:
                comp = self.compare_to_benchmark()
                summary['Excess Return'] = comp['excess_return']
                summary['Benchmark Sharpe'] = comp['benchmark_sharpe']
            except Exception:
                pass
                
        return summary

    def plot_performance(self, save_path: str = None):
        fig, axes = plt.subplots(2, 1, figsize=(12, 10), gridspec_kw={'height_ratios': [3, 1]})
        fig.suptitle('Portfolio Performance Summary', fontsize=16)

        ax1 = axes[0]
        pv = self.portfolio_value_history.copy()
        pv.index = pd.to_datetime(pv.index)
        portfolio_vals = pv['total_value']
        
        port_norm = (portfolio_vals / portfolio_vals.iloc[0]) * 100
        ax1.plot(port_norm.index, port_norm, label='Portfolio Strategy', color='blue', linewidth=2)

        if self.benchmark_series is not None:
            bench_vals = self.benchmark_series.copy()
            bench_vals.index = pd.to_datetime(bench_vals.index)
            bench_vals = bench_vals.reindex(port_norm.index).ffill().bfill()
            bench_norm = (bench_vals / bench_vals.iloc[0]) * 100
            ax1.plot(bench_norm.index, bench_norm, label='Benchmark', color='gray', linestyle='--', linewidth=1.5)

        buys = self.trades_history[self.trades_history['transaction_type'].isin(['BUY', 'COVER'])]
        sells = self.trades_history[self.trades_history['transaction_type'].isin(['SELL', 'SHORT'])]

        # Add buys/covers (green up arrows)
        if not buys.empty:
            for idx, row in buys.iterrows():
                dt = pd.to_datetime(row['transaction_datetime']).normalize()
                if dt in pv.index:
                    label = row['transaction_type'].capitalize() if idx == buys.index[0] else ""
                    ax1.scatter(dt, pv.loc[dt, 'total_value'] / portfolio_vals.iloc[0] * 100,
                                marker='^', color='green', s=100, label=label)

        # Add sells/shorts (red down arrows)
        if not sells.empty:
            for idx, row in sells.iterrows():
                dt = pd.to_datetime(row['transaction_datetime']).normalize()
                if dt in pv.index:
                    label = row['transaction_type'].capitalize() if idx == sells.index[0] else ""
                    ax1.scatter(dt, pv.loc[dt, 'total_value'] / portfolio_vals.iloc[0] * 100,
                                marker='v', color='red', s=100, label=label)

        ax1.set_title('Normalized Value (Base 100)')
        ax1.set_ylabel('Value')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        ax2 = axes[1]
        drawdown_data = self.get_max_drawdown()['drawdown_series'].copy() * 100
        drawdown_data.index = pd.to_datetime(drawdown_data.index)
        ax2.fill_between(drawdown_data.index, drawdown_data, 0, color='red', alpha=0.3)
        ax2.plot(drawdown_data.index, drawdown_data, color='red', linewidth=1)
        ax2.set_title('Portfolio Drawdown (%)')
        ax2.set_ylabel('Drawdown %')
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
            print(f"Plot saved to {save_path}")
        else:
            plt.show()
