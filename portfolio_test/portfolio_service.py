import pandas as pd
import numpy as np


class PortfolioStatisticsService:
    def __init__(self, portfolio_manager, benchmark_series: pd.Series = None):
        """
        Initialize the Portfolio Statistics Service.

        This service calculates comprehensive performance metrics for a portfolio,
        including Sharpe ratio, drawdown analysis, win rate, and benchmark comparison.

        :param portfolio_manager: Instance of PortfolioManager class
                                 Must have these attributes/methods:
                                 - starting_cash (float): Initial capital amount
                                 - get_portfolio_value_history() → DataFrame with 'total_value' column
                                 - get_trades_history() → DataFrame with trade records
                                 - get_current_portfolio_value() → float

        :param benchmark_series: Optional pd.Series with benchmark prices (e.g., S&P 500)
                                 Index should be dates, values should be prices.
                                 If provided, enables benchmark comparison features.
                                 If None, comparison methods will raise ValueError.
        """
        self.pm = portfolio_manager
        self.benchmark_series = self._validate_benchmark(benchmark_series)
        self.portfolio_value_history = self.pm.get_portfolio_value_history()
        self.trades_history = self.pm.get_trades_history()


    def _validate_benchmark(self, benchmark_series):
        """Validate and normalize benchmark series."""
        if benchmark_series is None:
            return None
        series = benchmark_series.copy()
        series.index = pd.to_datetime(series.index, errors='raise')
        series.index = pd.DatetimeIndex(series.index).normalize()
        series = series.sort_index(ascending=True)
        return series


    def get_total_valuation(self) -> float:
        """
        Get the current portfolio valuation.
        Returns the most recent portfolio total value.
        """
        return self.pm.get_current_portfolio_value()

    def get_portfolio_returns(self) -> pd.Series:
        """
        Calculate daily portfolio returns based on portfolio value history.
        Returns a pandas Series of daily returns.
        """
        portfolio_values = self.portfolio_value_history['total_value'].copy()
        daily_returns = portfolio_values.pct_change().dropna()
        return daily_returns

    def get_benchmark_returns(self) -> pd.Series:
        """
        Calculate daily benchmark returns.
        Returns a pandas Series of daily returns from the benchmark.
        """
        if self.benchmark_series is None:
            raise ValueError("Benchmark series not provided.")
        daily_returns = self.benchmark_series.pct_change().dropna()
        return daily_returns

    def get_sharpe_ratio(self, risk_free_rate: float = 0.02) -> dict:
        """
        Calculate the Sharpe Ratio for the portfolio.

        :param risk_free_rate: Annual risk-free rate (default 2%)
        :return: Dictionary with annualized return, volatility, and sharpe ratio
        """
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
        """
        Calculate the Sharpe Ratio for the benchmark.

        :param risk_free_rate: Annual risk-free rate (default 2%)
        :return: Dictionary with annualized return, volatility, and sharpe ratio
        """
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
        """
        Calculate total return from start to end.
        Returns the percentage gain/loss.
        """
        starting_value = self.pm.starting_cash
        ending_value = self.get_total_valuation()
        return (ending_value - starting_value) / starting_value

    def get_benchmark_total_return(self) -> float:
        """
        Calculate total return of the benchmark.
        Returns the percentage gain/loss.
        """
        if self.benchmark_series is None:
            raise ValueError("Benchmark series not provided.")

        start_price = self.benchmark_series.iloc[0]
        end_price = self.benchmark_series.iloc[-1]
        return (end_price - start_price) / start_price

    def get_max_drawdown(self) -> dict:
        """
        Calculate the maximum drawdown of the portfolio.

        :return: Dictionary with max drawdown percentage and period info
        """
        portfolio_values = self.portfolio_value_history['total_value'].copy()

        # Calculate running maximum
        running_max = portfolio_values.expanding().max()

        # Calculate drawdown
        drawdown = (portfolio_values - running_max) / running_max

        max_drawdown = drawdown.min()
        max_drawdown_date = drawdown.idxmin()

        return {
            'max_drawdown': max_drawdown,
            'max_drawdown_date': max_drawdown_date,
            'drawdown_series': drawdown
        }

    def get_benchmark_max_drawdown(self) -> dict:
        """
        Calculate the maximum drawdown of the benchmark.

        :return: Dictionary with max drawdown percentage and period info
        """
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
        Calculate the win rate based on trades.

        :return: Dictionary with win rate and trade statistics
        """
        trades = self.trades_history[self.trades_history['transaction_type'].isin(['BUY', 'SELL'])].copy()

        if len(trades) == 0:
            return {'win_rate': 0, 'winning_trades': 0, 'losing_trades': 0, 'total_trades': 0}

        # Simple approach: match buy/sell pairs and check if they're profitable
        buy_trades = trades[trades['transaction_type'] == 'BUY'].copy()
        sell_trades = trades[trades['transaction_type'] == 'SELL'].copy()

        winning_trades = 0
        losing_trades = 0

        for ticker in buy_trades['ticker'].unique():
            buy_price = buy_trades[buy_trades['ticker'] == ticker]['stock_price'].values
            sell_price = sell_trades[sell_trades['ticker'] == ticker]['stock_price'].values

            if len(buy_price) > 0 and len(sell_price) > 0:
                # Simple: compare most recent buy to most recent sell
                if sell_price[-1] > buy_price[-1]:
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
        """
        Compare portfolio performance vs benchmark.

        :return: Dictionary with comparison metrics
        """
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
        """
        Print a comprehensive performance summary of the portfolio.

        Displays:
        - Portfolio metrics (returns, volatility, Sharpe ratio, drawdown)
        - Trade statistics (total trades, win rate)
        - Benchmark comparison (if benchmark is provided)
        - Overall assessment and recommendations
        """
        print("\n" + "=" * 90)
        print("PORTFOLIO PERFORMANCE SUMMARY".center(90))
        print("=" * 90)

        # ===== PORTFOLIO METRICS =====
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

        # ===== TRADE STATISTICS =====
        win_rate_stats = self.get_win_rate()
        print(f"\n📈 TRADE STATISTICS:")
        print(f"   ├─ Total Trades:            {win_rate_stats['total_trades']:>15d}")
        print(f"   ├─ Winning Trades:          {win_rate_stats['winning_trades']:>15d}")
        print(f"   ├─ Losing Trades:           {win_rate_stats['losing_trades']:>15d}")
        print(f"   └─ Win Rate:                {win_rate_stats['win_rate']*100:>14.2f}%")

        # ===== BENCHMARK COMPARISON =====
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

            # ===== VERDICT =====
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
