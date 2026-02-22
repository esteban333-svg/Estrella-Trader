import asyncio
import json
import threading
import time
from datetime import datetime, timezone
from typing import Optional, Tuple
import urllib.request

import pandas as pd

WEBSOCKETS_AVAILABLE = False
try:
    import websockets  # type: ignore

    WEBSOCKETS_AVAILABLE = True
except Exception:
    websockets = None


class BinanceLiveStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._df = pd.DataFrame()
        self.last_update_utc: Optional[datetime] = None
        self.symbol: Optional[str] = None
        self.interval: Optional[str] = None

    def seed(self, df: pd.DataFrame, symbol: str, interval: str) -> None:
        with self._lock:
            self._df = df.copy()
            self.symbol = symbol
            self.interval = interval
            self.last_update_utc = datetime.now(timezone.utc)

    def update_from_kline(self, k: dict) -> None:
        try:
            ts = datetime.fromtimestamp(k.get("t", 0) / 1000, tz=timezone.utc)
            row = {
                "Open": float(k.get("o")),
                "High": float(k.get("h")),
                "Low": float(k.get("l")),
                "Close": float(k.get("c")),
                "Volume": float(k.get("v")),
            }
        except Exception:
            return

        with self._lock:
            if self._df.empty:
                self._df = pd.DataFrame([row], index=[ts])
            elif ts in self._df.index:
                self._df.loc[ts, ["Open", "High", "Low", "Close", "Volume"]] = [
                    row["Open"],
                    row["High"],
                    row["Low"],
                    row["Close"],
                    row["Volume"],
                ]
            else:
                self._df = pd.concat([self._df, pd.DataFrame([row], index=[ts])])
                self._df = self._df.tail(500)

            self.last_update_utc = datetime.now(timezone.utc)

    def get_df(self) -> pd.DataFrame:
        with self._lock:
            return self._df.copy()


def fetch_klines(symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
    try:
        url = (
            "https://api.binance.com/api/v3/klines"
            f"?symbol={symbol.upper()}&interval={interval}&limit={limit}"
        )
        with urllib.request.urlopen(url, timeout=10) as resp:
            raw = resp.read()

        data = json.loads(raw.decode("utf-8"))
        rows = []
        for k in data:
            ts = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
            rows.append(
                {
                    "ts": ts,
                    "Open": float(k[1]),
                    "High": float(k[2]),
                    "Low": float(k[3]),
                    "Close": float(k[4]),
                    "Volume": float(k[5]),
                }
            )

        df = pd.DataFrame(rows).set_index("ts")
        return df
    except Exception:
        return pd.DataFrame()


async def _ws_consume(symbol: str, interval: str, store: BinanceLiveStore, stop_event: threading.Event) -> None:
    if websockets is None:
        return

    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"
    while not stop_event.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                async for message in ws:
                    if stop_event.is_set():
                        break
                    payload = json.loads(message)
                    k = payload.get("k")
                    if k:
                        store.update_from_kline(k)
        except Exception:
            await asyncio.sleep(2)


def start_stream(symbol: str, interval: str, store: BinanceLiveStore) -> Tuple[threading.Thread, threading.Event]:
    stop_event = threading.Event()

    def runner() -> None:
        asyncio.run(_ws_consume(symbol, interval, store, stop_event))

    thread = threading.Thread(target=runner, name="binance-ws", daemon=True)
    thread.start()
    return thread, stop_event
