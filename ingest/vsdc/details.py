"""Save VSDC corporate-action notice bodies to local Bronze."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from hashlib import sha256
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
import re
from threading import local
import time

import pandas as pd
import requests

from ingest.market.lake import TZ
from ingest.vsdc.adapter import RequestPacer
from ingest.vsdc.backfill import DEFAULT_ROOT, atomic_write

VSDC_LINK = re.compile(r"https://vsdc\.vn/(?:vi/)?ad1?/\d+$")
UPDATED = re.compile(r'class="time-newstcph"[^>]*>(.*?)</div>', re.S)
PAGE_DATE = re.compile(r"\d{2}/\d{2}/\d{4}(?:\s*-\s*\d{2}:\d{2}:\d{2})?")
BATCH_SIZE = 100


class Article(HTMLParser):
    def __init__(self, page: str) -> None:
        super().__init__()
        self.lines = [0]
        for match in re.finditer("\n", page):
            self.lines.append(match.end())
        self.start = self.end = None
        self.depth = 0

    def character_index(self) -> int:
        line, column = self.getpos()
        return self.lines[line - 1] + column

    def handle_starttag(self, tag, attrs):
        if tag != "div" or self.end is not None:
            return
        if self.depth or "content-category" in dict(attrs).get("class", "").split():
            if not self.depth:
                self.start = self.character_index() + len(self.get_starttag_text())
            self.depth += 1

    def handle_endtag(self, tag):
        if tag == "div" and self.depth:
            self.depth -= 1
            if self.depth == 0:
                self.end = self.character_index()


def parse_notice(page: str) -> tuple[str, str]:
    article = Article(page)
    article.feed(page)
    if article.start is None or article.end is None:
        raise ValueError("Missing VSDC article body")
    body = page[article.start:article.end].strip()
    if not body:
        raise ValueError("Empty VSDC article body")
    timestamp = UPDATED.search(page)
    value = PAGE_DATE.search(unescape(timestamp.group(1))) if timestamp else None
    return body, value.group(0) if value else ""


def fetch_notice(job: dict, sessions: local, pace: RequestPacer) -> dict:
    base = {**job, "source": "vsdc", "fetched_at": datetime.now(TZ).isoformat()}
    if not VSDC_LINK.fullmatch(job["source_url"]):
        return dict(base, status="FAILED", error="Unrecognized VSDC notice URL")
    if not hasattr(sessions, "http"):
        sessions.http = requests.Session()
    for attempt in range(3):
        try:
            pace.wait()
            url = job["source_url"]
            if attempt != 1:
                url = url.replace("https://vsdc.vn/ad", "https://vsdc.vn/vi/ad", 1)
            response = sessions.http.get(url, timeout=(8, 30))
            response.raise_for_status()
            body, updated = parse_notice(response.text)
            return dict(base, status="OK", error="", article_html=body, source_updated_at_text=updated,
                        body_sha256=sha256(body.encode()).hexdigest())
        except (requests.RequestException, ValueError) as exc:
            status = exc.response.status_code if isinstance(exc, requests.HTTPError) else None
            if attempt == 2 or status not in {None, 429, 500, 502, 503, 504}:
                error = f"{exc} ({len(response.content)} bytes at {response.url})" if isinstance(exc, ValueError) else str(exc)
                return dict(base, status="FAILED", error=error)
            sessions.http.close()
            sessions.http = requests.Session()
            time.sleep(3 * (attempt + 1))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--pause", type=float, default=0.18)
    args = parser.parse_args()
    if args.workers < 1 or not 0 <= args.pause < float("inf"):
        parser.error("Invalid --workers or --pause")

    calendar_report = pd.read_csv(args.coverage_report)
    if not calendar_report.status.eq("OK").all():
        parser.error("Finish the calendar crawl before fetching notice details")
    calendar = pd.concat([pd.read_parquet(path) for path in calendar_report.local_path], ignore_index=True)
    revisions = calendar.title.str.contains(r"điều chỉnh|đính chính|h[ủuỷ]y|thay đổi|hoãn", case=False, regex=True)
    eligible = calendar.loc[calendar.event_types.ne("OTHER") | revisions].drop_duplicates("notice_id")
    eligible = eligible.sort_values(["record_date", "notice_id"])
    key = args.coverage_report.stem.removeprefix("coverage_")
    root = args.output_dir.resolve()
    output = root / "notice_details" / "source=vsdc" / f"request={key}"
    report_path = root / "_reports" / f"notice_details_{key}.csv"
    pace, sessions = RequestPacer(args.pause), local()
    totals = []
    print(f"VSDC notice details: {len(eligible)} notices", flush=True)
    columns = ["notice_id", "source_url", "record_date", "symbol", "title", "event_types"]
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for year, records in eligible.groupby(eligible.record_date.dt.year):
            for offset in range(0, len(records), BATCH_SIZE):
                group = records.iloc[offset: offset + BATCH_SIZE]
                path = output / f"record_year={year}" / f"part-{offset:05d}.parquet"
                previous = pd.read_parquet(path) if path.is_file() else pd.DataFrame(columns=["notice_id", "status"])
                valid = previous.loc[previous.status.eq("OK") & previous.notice_id.isin(group.notice_id)]
                pending = group.loc[~group.notice_id.isin(valid.notice_id), columns].to_dict("records")
                final = valid
                if pending:
                    results = list(pool.map(lambda item: fetch_notice(item, sessions, pace), pending))
                    final = pd.concat([valid, pd.DataFrame(results)], ignore_index=True) if not valid.empty else pd.DataFrame(results)
                    final = final.sort_values(["record_date", "notice_id"])
                    atomic_write(path, lambda temp: final.to_parquet(temp, index=False))
                ok = int(final.status.eq("OK").sum())
                totals.append(dict(year=year, offset=offset, expected=len(group), saved=ok,
                                   failed=len(group)-ok, local_path=path))
                atomic_write(report_path, lambda temp: pd.DataFrame(totals).to_csv(temp, index=False))
                print(f"[{year} {offset:05d}] {ok}/{len(group)} saved", flush=True)
    print(f"Coverage: {report_path}", flush=True)
    return int(any(item["failed"] for item in totals))
