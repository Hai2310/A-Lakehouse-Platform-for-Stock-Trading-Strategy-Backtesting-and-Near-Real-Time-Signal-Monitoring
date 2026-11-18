"""Fetch a small VCI daily-price sample and store it as a Bronze Parquet file."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from vnstock import Quote


TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def crawl_sample(symbol: str = "FPT", days: int = 14) -> Path:
    load_dotenv()
    end = datetime.now(TZ).date()
    start = end - timedelta(days=days)

    quote = Quote(symbol=symbol, source="VCI", show_log=False)
    frame = quote.history(
        start=start.isoformat(),
        end=end.isoformat(),
        interval="1D",
    )
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)
    if frame.empty:
        raise RuntimeError(
            f"Vnstock returned no rows for {symbol} from {start} through {end}."
        )

    frame = frame.copy()
    frame["symbol"] = symbol.upper()
    frame["asset"] = "equity"
    frame["source"] = "VCI"
    frame["interval"] = "1D"

    data_root = Path(os.getenv("MARKET_DATA_DIR", "data/bronze/market"))
    output_dir = (
        data_root
        / "daily_ohlc"
        / "source=vci"
        / "interval=1D"
        / f"symbol={symbol.upper()}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{symbol.upper()}_1D_{start}_{end}.parquet"
    frame.to_parquet(output_path, index=False)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="FPT", help="Vietnamese stock ticker")
    parser.add_argument(
        "--days", type=int, default=14, help="Calendar days to request (default: 14)"
    )
    args = parser.parse_args()
    if args.days < 1:
        parser.error("--days must be at least 1")

    output_path = crawl_sample(args.symbol.upper(), args.days)
    frame = pd.read_parquet(output_path)
    print(f"Saved {len(frame)} rows to {output_path}")
    print(f"Columns: {', '.join(map(str, frame.columns))}")


if __name__ == "__main__":
    main()
