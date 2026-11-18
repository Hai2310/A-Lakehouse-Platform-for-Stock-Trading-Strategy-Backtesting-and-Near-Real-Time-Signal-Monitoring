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
