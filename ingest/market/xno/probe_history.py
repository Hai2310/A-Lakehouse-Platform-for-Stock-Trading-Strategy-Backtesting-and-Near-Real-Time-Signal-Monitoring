"""Measure the full history returned by XNO; preserve raw responses locally."""
from __future__ import annotations

import argparse
import contextlib
import io
import os
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv

from transform.silver.normalization import normalize_ohlcv

ROOT = Path(__file__).resolve().parents[3]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbols', default='FPT,HPG,VCB,SSI,VNM')
    parser.add_argument('--futures', default='VN30F1M')
    args = parser.parse_args()
    load_dotenv(ROOT / '.env')
    key = os.getenv('XNO_API_KEY', '').strip()
    if not key:
        print('MISSING_KEY: Điền XNO_API_KEY trong .env rồi chạy lại. Chưa gọi API.')
        return 2

    from xnoapi.vn.data import client, get_stock_hist, get_derivatives_hist

    client(apikey=key, verify=False)
    run_id = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).strftime('%Y%m%dT%H%M%S%f')
    output = ROOT / 'data' / 'xno_probe' / run_id
    output.mkdir(parents=True)
    jobs = [
        ('equity', symbol.strip().upper(), resolution)
        for symbol in args.symbols.split(',') if symbol.strip()
        for resolution in ('m', 'h')
    ] + [
        ('futures', symbol.strip().upper(), interval)
        for symbol in args.futures.split(',') if symbol.strip()
        for interval in ('1m', '5m', '1D')
    ]
    report = []
    original_request = requests.sessions.Session.request

    def bounded_request(self, *positional, **kwargs):
        kwargs.setdefault('timeout', (10, 120))
        return original_request(self, *positional, **kwargs)

    for asset, symbol, resolution in jobs:
        record = dict(asset=asset, symbol=symbol, resolution=resolution,
                      raw_rows=0, normalized_rows=0, first_time='', last_time='',
                      trading_days=0, duplicate_times=0, status='FAILED', error_type='')
        try:
            # Library output is suppressed to avoid exposing credentials in errors.
            with patch.object(requests.sessions.Session, 'request', bounded_request), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                raw = (get_stock_hist(symbol, resolution=resolution)
                       if asset == 'equity' else get_derivatives_hist(symbol, resolution))
            if isinstance(raw, dict) and raw.get('error'):
                raise RuntimeError('XNO returned an error payload')
            if not isinstance(raw, pd.DataFrame):
                raw = pd.DataFrame(raw)
            prefix = output / f'{asset}-{symbol}-{resolution}'
            raw.to_parquet(prefix.with_suffix('.raw.parquet'), index=False)
            record['raw_rows'] = len(raw)
            if raw.empty:
                record['status'] = 'EMPTY'
            else:
                bars = normalize_ohlcv(raw)
                bars.to_parquet(prefix.with_suffix('.normalized.parquet'), index=False)
                timestamps = bars['time'].dropna()
                record.update(normalized_rows=len(bars),
                              first_time=str(timestamps.min()) if len(timestamps) else '',
                              last_time=str(timestamps.max()) if len(timestamps) else '',
                              trading_days=timestamps.dt.date.nunique(),
                              duplicate_times=int(timestamps.duplicated().sum()),
                              status='OK' if len(bars) else 'NORMALIZATION_EMPTY')
        except Exception as exc:
            record['error_type'] = type(exc).__name__
        report.append(record)
        pd.DataFrame(report).to_csv(output / 'coverage.csv', index=False)
        print(f"[{record['status']}] {symbol} {resolution}: {record['raw_rows']} rows | {record['first_time']} -> {record['last_time']}", flush=True)
        time.sleep(1)
    print(f'Report: {output / "coverage.csv"}')
    return 1 if any(r['status'] == 'FAILED' for r in report) else 0


if __name__ == '__main__':
    raise SystemExit(main())
