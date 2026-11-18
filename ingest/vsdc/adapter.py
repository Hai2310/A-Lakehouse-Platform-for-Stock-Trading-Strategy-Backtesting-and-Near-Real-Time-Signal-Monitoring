"""Read the public VSDC calendar and its original notice links."""

from datetime import date, datetime
from html.parser import HTMLParser
import re
from threading import Lock
import time
from urllib.parse import urljoin

import requests

BASE_URL = "https://vsdc.vn"
CALENDAR = f"{BASE_URL}/vi/lich-giao-dich"
PAGE_SIZE = 10
TOKEN = re.compile(r'<meta name="__VPToken" content="([^"]+)"')
TOTAL = re.compile(r'id=[\'\"]d_total_rec[\'\"][^>]*>\s*Hiển thị:\s*([\d.]+)\s*-\s*([\d.]+)\s*/\s*([\d.]+)')
NOTICE = re.compile(r"/(?:vi/)?ad1?/(\d+)$")


class CalendarTable(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.active = False
        self.found = False
        self.rows = []
        self.cells = []
        self.fragments = None
        self.href = ""

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "table" and attrs.get("id") == "tblLichTHQ":
            self.active = self.found = True
        if not self.active:
            return
        if tag == "tr":
            self.cells, self.href = [], ""
        elif tag == "td":
            self.fragments = []
        elif tag == "a" and len(self.cells) == 4:
            self.href = attrs.get("href", "")

    def handle_data(self, data):
        if self.fragments is not None:
            self.fragments.append(data)

    def handle_endtag(self, tag):
        if not self.active:
            return
        if tag == "td" and self.fragments is not None:
            self.cells.append(" ".join("".join(self.fragments).split()))
            self.fragments = None
        elif tag == "tr" and self.cells:
            self.rows.append((self.cells, self.href))
        elif tag == "table":
            self.active = False


def parse_calendar(html: str) -> tuple[list[dict], int]:
    table = CalendarTable()
    table.feed(html)
    summary = TOTAL.search(html)
    if not table.found or not summary:
        raise ValueError("Unexpected VSDC calendar markup or missing pagination total")
    first, last, total = (int(value.replace(".", "")) for value in summary.groups())
    if len(table.rows) != (last - first + 1 if total else 0):
        raise ValueError("Calendar row count does not match VSDC pagination")
    rows = []
    for position, (cells, href) in enumerate(table.rows, first):
        ordinal, record_date, symbol, isin, title, security_type, market, management_area = cells
        notice = NOTICE.fullmatch(href)
        if int(ordinal) != position or not notice or not symbol or not title:
            raise ValueError("VSDC calendar row lacks stable notice metadata")
        rows.append(dict(
            ordinal=position, notice_id=notice.group(1), source_url=urljoin(BASE_URL, href),
            record_date=datetime.strptime(record_date, "%d/%m/%Y").date(), symbol=symbol.upper(),
            isin=isin, title=title, security_type=security_type, market=market, management_area=management_area,
        ))
    return rows, total


class RequestPacer:
    def __init__(self, pause: float) -> None:
        self.pause = pause
        self.lock = Lock()
        self.next_request = 0.0

    def wait(self) -> None:
        with self.lock:
            time.sleep(max(0, self.next_request - time.monotonic()))
            self.next_request = time.monotonic() + self.pause


class VSDCCalendar:
    def __init__(self, session: requests.Session, pace: RequestPacer) -> None:
        self.session = session
        self.pace = pace
        self.headers = None

    def page(self, start: date, end: date, number: int) -> tuple[list[dict], int]:
        payload = {
            "SearchKey": f"|1||{start:%d/%m/%Y}|{end:%d/%m/%Y}|VI",
            "CurrentPage": number, "RecordOnPage": PAGE_SIZE,
            "OrderBy": "NgayDkcc", "OrderType": "ASC",
        }
        for attempt in range(3):
            try:
                if self.headers is None:
                    self.pace.wait()
                    landing = self.session.get(CALENDAR, params={"tab": "LICH_THQ", "date": end.strftime("%d/%m/%Y")}, timeout=(8, 30))
                    landing.raise_for_status()
                    token = TOKEN.search(landing.text)
                    if not token:
                        raise ValueError("VSDC calendar did not issue a public session token")
                    self.headers = {"__VPToken": token.group(1), "X-Requested-With": "XMLHttpRequest", "Referer": landing.url}
                self.pace.wait()
                result = self.session.post(f"{BASE_URL}/lich-thq/search", json=payload,
                                           headers=self.headers, timeout=(8, 30))
                result.raise_for_status()
                return parse_calendar(result.text)
            except requests.RequestException as exc:
                status = exc.response.status_code if exc.response is not None else None
                if attempt == 2 or status not in {None, 400, 429, 500, 502, 503, 504}:
                    raise
                self.headers = None
                time.sleep(3 * (attempt + 1))
