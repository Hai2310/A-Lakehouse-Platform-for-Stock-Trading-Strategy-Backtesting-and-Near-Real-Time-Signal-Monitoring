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
