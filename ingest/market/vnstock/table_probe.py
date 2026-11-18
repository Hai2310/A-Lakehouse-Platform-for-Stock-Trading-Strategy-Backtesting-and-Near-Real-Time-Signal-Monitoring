"""Read the remaining market tables in memory and report missing source coverage."""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock
from zoneinfo import ZoneInfo

import pandas as pd


def safe_message(exc: Exception) -> str:
    message = str(exc)
    for name, value in os.environ.items():
        if any(word in name.upper() for word in ('KEY', 'SECRET', 'TOKEN', 'PASSWORD')) and len(value) > 5:
            message = message.replace(value, '[REDACTED]')
    return message


def run_remaining_tables(symbols: list[str], *, start: str, end: str,
                         indices: tuple[str, ...] = ('VNINDEX', 'VN30', 'HNXINDEX', 'UPCOMINDEX')):
    """Fetch seven tables, retaining frames and per-request errors only in RAM.

    PARTIAL means the returned columns or catalogue are not a full replacement
    for the table. Current snapshots do not imply point-in-time historical coverage.
    """
    from vnstock import Market, Reference
    import vnstock_oapi as oapi

    tables: dict[str, pd.DataFrame] = {}
    records: list[dict] = []
    failures: list[dict] = []
    now = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).isoformat()
    symbols = list(dict.fromkeys(symbols))
    configured = {
        'ssi': all(os.getenv(k, '').strip() for k in ('SSI_API_KEY', 'SSI_API_SECRET')),
        'fhsc': bool(os.getenv('FHSC_API_KEY', '').strip()),
    }

    def request(table, source, key, fn):
        try:
            frame = fn()
            if not isinstance(frame, pd.DataFrame):
                raise TypeError('Expected DataFrame')
            return frame
        except Exception as exc:
            failures.append(dict(table=table, source=source, key=key,
                                 error_type=type(exc).__name__, message=safe_message(exc)))
            return pd.DataFrame()

    def finish(table, frame, source, status='OK', note=''):
        tables[table] = frame
        if status == 'OK' and frame.empty:
            status = 'EMPTY'
        records.append(dict(table=table, source=source, status=status, rows=len(frame), note=note))
        print(f'[{status}] {table}: {len(frame):,} rows ({source})', flush=True)

    catalog = request('securities', 'dnse', 'all', lambda: Reference('dnse').equity.list(type='stock'))
    if 'symbol' in catalog:
        catalog = catalog.loc[catalog.symbol.isin(symbols)].copy()
    finish('securities', catalog, 'dnse', note=f'Current listing; selected {len(symbols)} symbols only.')

    # Global pacing covers these batches, rather than starting every worker at once.
    lock = Lock()
    next_request = [0.0]
    def pace():
        with lock:
            wait = next_request[0] - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            next_request[0] = time.monotonic() + 0.25

    def batch(table, source, keys, fn):
        frames = []
        def one(key):
            for attempt in range(3):
                pace()
                try:
                    return key, fn(key), None
                except Exception as exc:
                    message = safe_message(exc)
                    if attempt < 2 and any(s in message.lower() for s in ('429', 'rate limit', 'rate_limit', 'too many')):
                        time.sleep(30 * (attempt + 1))
                        continue
                    return key, None, (type(exc).__name__, message)
        with ThreadPoolExecutor(max_workers=4) as pool:
            pending = [pool.submit(one, key) for key in keys]
            for count, future in enumerate(as_completed(pending), 1):
                key, frame, error = future.result()
                if error:
                    failures.append(dict(table=table, source=source, key=key, error_type=error[0], message=error[1]))
                elif frame is not None and not frame.empty:
                    frames.append(frame)
                if count % 100 == 0 or count == len(keys):
                    print(f'{table}: {count}/{len(keys)} requests', flush=True)
        return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()

    def instrument(symbol):
        envelopes = oapi.source('dnse').raw('price/{symbol}/secdef', symbol=symbol)
        rows = []
        for envelope in envelopes:
            body = envelope.body
            if isinstance(body, list):
                rows.extend(body)
            else:
                raise TypeError('Unexpected secdef response; inspect source schema.')
        frame = pd.DataFrame(rows)
        if not frame.empty:
            frame['symbol'] = symbol
            frame['snapshot_at'] = now
        return frame

    # One request per symbol serves both trading metadata and price-limit snapshot.
    instruments = batch('instrument_snapshot', 'dnse', symbols, instrument)
    if configured['fhsc']:
        def profile(symbol):
            frame = Reference('fhsc').company(symbol).info().copy()
            if not frame.empty:
                frame['symbol'] = symbol
                frame['snapshot_at'] = now
            return frame
        details = batch('securities_details', 'fhsc', symbols, profile)
        finish('securities_details', details, 'fhsc',
               status='PARTIAL' if any(f['table'] == 'securities_details' for f in failures) else 'OK',
               note='Current company profiles; not historical corporate SCD2.')
    else:
        finish('securities_details', instruments, 'dnse', status='PARTIAL',
               note='Trading definitions only. Add FHSC_API_KEY for company profiles.')

    if configured['fhsc']:
        prices = request('daily_stock_price', 'fhsc', 'selected',
                         lambda: Market('fhsc').equity.quote(','.join(symbols)))
        finish('daily_stock_price', prices, 'fhsc', note='Current snapshot, not 10 years of snapshots.')
    else:
        # Keep the native definitions; do not invent last/close price or volume.
        finish('daily_stock_price', instruments.copy(), 'dnse', status='PARTIAL',
               note='Current reference/ceiling/floor only; no last price/volume. Add FHSC_API_KEY for full quote snapshot.')

    index_source = 'ssi' if configured['ssi'] else ('fhsc' if configured['fhsc'] else None)
    if index_source:
        index_list = request('index_list', index_source, 'all', lambda: Reference(index_source).index.list())
        finish('index_list', index_list, index_source)
    else:
        groups = request('index_list', 'builtin', 'groups', lambda: Reference().index.groups())
        # Builtin includes funds, exchanges and futures groups; keep only actual index groups.
        if 'category' in groups:
            groups = groups.loc[groups.category.eq('Chỉ số VN')].copy()
        finish('index_list', groups, 'builtin', status='PARTIAL',
               note='Builtin index groups, not full index catalogue. Add SSI_API_KEY/SSI_API_SECRET or FHSC_API_KEY.')

    if configured['ssi']:
        members = []
        for index in indices:
            frame = request('index_components', 'ssi', index, lambda index=index: Reference('ssi').index(index).members())
            if not frame.empty:
                frame = frame.assign(index_symbol=index, snapshot_at=now)
                members.append(frame)
        components = pd.concat(members, ignore_index=True) if members else pd.DataFrame()
        finish('index_components', components, 'ssi',
               status='PARTIAL' if any(f['table'] == 'index_components' for f in failures) else 'OK',
               note='Current members only; some indices may not be supported. Not historical constituents.')
    else:
        finish('index_components', pd.DataFrame(), 'ssi', status='MISSING_CREDENTIALS',
               note='Add SSI_API_KEY and SSI_API_SECRET. No substitute generated from current stock catalogue.')

    for table, intervals in [('daily_index', ('1D',)), ('intraday_index', ('5m', '1m'))]:
        frames = []
        expected = len(indices) * len(intervals)
        successes = 0
        for index in indices:
            for interval in intervals:
                frame = request(table, 'dnse', f'{index}/{interval}',
                                lambda index=index, interval=interval: Market('dnse').index(index).ohlcv(
                                    start=start, end=end, interval=interval, compat='v5'))
                if not frame.empty:
                    frames.append(frame.assign(symbol=index, interval=interval, source='dnse', asset='index'))
                    successes += 1
                time.sleep(0.25)
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        finish(table, combined, 'dnse', status='OK' if successes == expected else 'PARTIAL',
               note=f'{successes}/{expected} index/interval series returned data; requested {start} to {end}.')

    # Report basic input coverage separately from column/schema completeness.
    report = pd.DataFrame(records)
    report['failed_requests'] = [
        sum(f['table'] == table or (
            table in {'securities_details', 'daily_stock_price'} and f['table'] == 'instrument_snapshot'
        ) for f in failures) for table in report.table
    ]
    errors = pd.DataFrame(failures, columns=['table', 'source', 'key', 'error_type', 'message'])
    return tables, report, errors


def load_local_reference(bronze_root):
    """Read existing snapshots for testing without treating them as current data.

    Return them separately: never silently replace a live table with a stale snapshot.
    """
    from pathlib import Path
    frames = {}
    report = []
    keys = {
        'index_list': ['source', 'symbol'],
        'index_components': ['source', 'index_symbol', 'symbol'],
        'securities_details': ['source', 'symbol'],
        'daily_stock_price': ['source', 'symbol'],
    }
    for table, identity in keys.items():
        parts = []
        errors = 0
        for path in sorted((Path(bronze_root) / table).rglob('*.parquet')):
            try:
                frame = pd.read_parquet(path)
                frame['_local_file'] = str(path)
                parts.append(frame)
            except Exception:
                errors += 1
        data = pd.concat(parts, ignore_index=True, sort=False) if parts else pd.DataFrame()
        if not data.empty:
            if 'ingested_at' in data:
                data = data.sort_values('ingested_at', kind='stable')
            identity = [column for column in identity if column in data]
            if identity:
                data = data.drop_duplicates(identity, keep='last')
            data['_coverage_status'] = 'EXISTING_SNAPSHOT_NOT_VERIFIED_CURRENT'
        frames[table] = data
        report.append(dict(
            table=table, rows=len(data),
            symbols=data['symbol'].nunique() if 'symbol' in data else 0,
            source_snapshot_latest=str(data['ingested_at'].max()) if 'ingested_at' in data else '',
            status='EXISTING_SNAPSHOT' if not data.empty else 'NO_LOCAL_DATA',
            unreadable_files=errors,
        ))
    return frames, pd.DataFrame(report)


def load_exchange_components(csv_path):
    """Load a transcription of exchange notices, preserving each effective date."""
    fields = ['index_symbol', 'symbol', 'effective_from', 'source_url']
    data = pd.read_csv(csv_path, dtype='string')
    missing = [field for field in fields if field not in data]
    if missing:
        raise ValueError('Missing constituent CSV columns: ' + ', '.join(missing))

    for field in fields:
        data[field] = data[field].str.strip()
    if data[fields].fillna('').eq('').any().any():
        raise ValueError('Each constituent needs an index, symbol, effective date and source URL.')
    for field in ('index_symbol', 'symbol'):
        data[field] = data[field].str.upper()
    if not data['effective_from'].str.fullmatch(r'\d{4}-\d{2}-\d{2}').all():
        raise ValueError('Use YYYY-MM-DD for effective_from.')
    data['effective_from'] = pd.to_datetime(data['effective_from'], format='%Y-%m-%d')
    if not data['source_url'].str.fullmatch(r'https?://[^\s]+').all():
        raise ValueError('Provide the URL of each published notice.')
    if data.duplicated(['index_symbol', 'symbol', 'effective_from']).any():
        raise ValueError('Duplicate index, symbol and effective date in constituent CSV.')
    return data.sort_values(['index_symbol', 'effective_from', 'symbol']).reset_index(drop=True)
