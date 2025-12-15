# python
"""TradeStore: simple SQLite-backed trade persistence and retrieval.

Provides:
- TradeStore.save_trade(...) to insert trades
- TradeStore.fetch_trades_for_ticker(ticker) -> pandas.DataFrame

Assumes `trades` table with columns:
    ticker TEXT,
    shares REAL,
    actual_price REAL,
    valued_price REAL,
    currency TEXT,
    buy_datetime INTEGER,
    sell_datetime INTEGER

Datetimes are integer seconds since epoch (or NULL).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Optional, Any

import pandas as pd

from portfolio_test.motor import CalculationMotor


class TradeStore:
    """SQLite-backed trade store.

    Example usage:
        ts = TradeStore('trades.db')
        ts.save_trade('AAPL', shares=1.5, actual_price=150.0, valued_price=160.0, currency='USD', buy_datetime=datetime.now())
        df = ts.fetch_trades_for_ticker('AAPL')
    """

    def __init__(self, db_path: str = "trades.db") -> None:
        self.db_path = db_path

    def _to_seconds(self, v: Optional[Any]) -> Optional[int]:
        if v is None:
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        if isinstance(v, datetime):
            return int(v.timestamp())
        try:
            return int(pd.to_datetime(v).timestamp())
        except Exception:
            return None

    def save_trade(self,
                   ticker: str,
                   shares: float,
                   actual_price: float,
                   valued_price: float,
                   currency: str,
                   buy_datetime: Optional[Any],
                   sell_datetime: Optional[Any] = None) -> None:
        """Insert a trade row into the database.

        buy_datetime and sell_datetime may be datetime, int seconds or parseable string.
        """
        bsec = self._to_seconds(buy_datetime)
        ssec = self._to_seconds(sell_datetime)
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO trades (ticker, shares, actual_price, valued_price, currency, buy_datetime, sell_datetime) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ticker, shares, actual_price, valued_price, currency, bsec, ssec)
        )
        conn.commit()
        conn.close()

    def fetch_trades_for_ticker(self, ticker: str) -> pd.DataFrame:
        """Return all rows for `ticker` as a DataFrame and attach a `fetched_price` column.

        - If a row has a non-null sell_datetime, attempt to get the close price at or immediately before that datetime.
        - If sell_datetime is null, use the latest available close price.
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            "SELECT ticker, shares, actual_price, valued_price, currency, buy_datetime, sell_datetime FROM trades WHERE ticker = ?",
            (ticker,)
        )
        rows = cur.fetchall()
        conn.close()

        if not rows:
            return pd.DataFrame(columns=[
                "ticker", "shares", "actual_price", "valued_price", "currency", "buy_datetime", "sell_datetime", "fetched_price"
            ])

        df = pd.DataFrame(rows, columns=[
            "ticker", "shares", "actual_price", "valued_price", "currency", "buy_datetime", "sell_datetime"
        ])

        # convert integer seconds to datetimes (or NaT)
        df["buy_datetime"] = pd.to_datetime(df["buy_datetime"], unit="s", errors="coerce")
        df["sell_datetime"] = pd.to_datetime(df["sell_datetime"], unit="s", errors="coerce")

        # instantiate CalculationMotor once for this ticker
        prices = pd.DataFrame()
        try:
            cm = CalculationMotor(ticker)
            if hasattr(cm, 'prices') and cm.prices is not None:
                prices = cm.prices
        except Exception:
            prices = pd.DataFrame()

        def _price_for_date(ts: Optional[pd.Timestamp]) -> Optional[float]:
            if prices.empty:
                return None
            if ts is None or pd.isna(ts):
                try:
                    return float(prices['Close'].iloc[-1])
                except Exception:
                    return None
            try:
                sel = prices.loc[:ts]
                if not sel.empty:
                    return float(sel['Close'].iloc[-1])
                sel2 = prices.loc[ts:]
                if not sel2.empty:
                    return float(sel2['Close'].iloc[0])
            except Exception:
                try:
                    close_series = prices['Close']
                    val = close_series.reindex([ts], method='ffill')
                    if not pd.isna(val.iloc[0]):
                        return float(val.iloc[0])
                except Exception:
                    pass
            return None

        df['fetched_price'] = df['sell_datetime'].apply(_price_for_date)

        return df

