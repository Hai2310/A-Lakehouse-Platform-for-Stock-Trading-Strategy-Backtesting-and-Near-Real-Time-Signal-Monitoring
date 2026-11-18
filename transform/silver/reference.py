"""Observed instrument identities and the small market reference tables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from transform.silver.market import _canonicalize

VENUES = {"STO": "HOSE", "STX": "HNX", "UPX": "UPCOM", "DVX": "HNXDS", "HSX": "HOSE"}
INDEX_VENUES = {"VNINDEX": "HOSE", "VN30": "HOSE", "VN100": "HOSE", "HNXINDEX": "HNX",
                "HNX30": "HNX", "UPCOMINDEX": "UPCOM"}
CATALOG_TABLES = ("securities", "securities_details", "daily_stock_price", "index_list", "index_components")


def read_table(path: Path) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Bronze Parquet in {path}")
    return pd.concat([pd.read_parquet(file).assign(bronze_file=str(file)) for file in files], ignore_index=True)


def series(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame[column] if column in frame else pd.Series(pd.NA, index=frame.index, dtype="string")


def venue(values: pd.Series) -> pd.Series:
    return values.astype("string").str.strip().str.upper().replace(VENUES).replace("", pd.NA)


def utc(values: pd.Series) -> pd.Series:
    """Interpret timezone-less source timestamps as exchange-local time."""
    if isinstance(values.dtype, pd.DatetimeTZDtype):
        return values.dt.tz_convert("UTC")
    values = values.astype("string")
    has_zone = values.str.contains(r"(?:Z|[+-][0-9]{2}:?[0-9]{2})$", case=False, na=False)
    result = pd.to_datetime(values.where(has_zone), utc=True, format="mixed", errors="coerce")
    local = pd.to_datetime(values.where(~has_zone), format="mixed", errors="coerce")
    return result.fillna(local.dt.tz_localize("Asia/Ho_Chi_Minh", ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC"))


def reference_frame(root: Path, table: str) -> pd.DataFrame:
    frame = _canonicalize(read_table(root / table), table)
    observed = utc(series(frame, "snapshot_at")).fillna(utc(series(frame, "ingested_at")))
    exchange = venue(series(frame, "exchange")).fillna(venue(series(frame, "marketid")))
    frame["exchange"] = exchange
    frame["observed_at_utc"] = observed
    frame["evidence_date"] = observed.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None).dt.normalize()
    return frame


class Identity:
    def __init__(self, catalog: pd.DataFrame, definitions: pd.DataFrame, calendar: pd.DataFrame) -> None:
        authoritative = catalog.loc[catalog.source.eq("dnse") & catalog.exchange.notna()]
        # Current listing takes precedence over old snapshots for the venue hint.
        distinct = authoritative[["symbol", "exchange"]].drop_duplicates()
        self.venues = distinct.loc[~distinct.symbol.duplicated(keep=False)].set_index("symbol").exchange
        fields = ["symbol", "exchange", "isin", "evidence_date", "observed_at_utc", "source", "bronze_file"]
        notices = calendar.assign(exchange=venue(calendar.market), observed_at_utc=utc(calendar.ingested_at),
                                  evidence_date=pd.to_datetime(calendar.record_date))
        evidence = pd.concat([frame.reindex(columns=fields) for frame in (definitions, notices, catalog)], ignore_index=True)
        evidence["isin"] = evidence["isin"].astype("string").str.upper().str.strip()
        evidence.loc[~evidence["isin"].str.fullmatch(r"VN[A-Z0-9]{10}", na=False), "isin"] = pd.NA
        pairs = evidence.dropna(subset=["symbol", "exchange", "isin"])[["symbol", "exchange", "isin"]].drop_duplicates()
        ambiguous = pairs.duplicated(["symbol", "exchange"], keep=False)
        self.by_pair = pairs.loc[~ambiguous].set_index(["symbol", "exchange"])["isin"]
        self.ambiguous = pairs.loc[ambiguous].set_index(["symbol", "exchange"]).index
        self.evidence = self.attach(evidence.dropna(subset=["symbol"]))

    def attach(self, frame: pd.DataFrame, *, allow_venue_hint=True) -> pd.DataFrame:
        result = frame.copy()
        symbol = result.symbol.astype("string").str.upper().str.strip().replace("", pd.NA)
        exchange = venue(series(result, "exchange"))
        if allow_venue_hint:
            exchange = exchange.fillna(symbol.map(self.venues))
            exchange = exchange.mask(series(result, "asset").eq("futures").fillna(False), "HNXDS")
        supplied = series(result, "isin").astype("string").str.upper().str.strip()
        supplied = supplied.where(supplied.str.fullmatch(r"VN[A-Z0-9]{10}", na=False))
        lookup = pd.MultiIndex.from_arrays([symbol, exchange])
        matched = pd.Series(self.by_pair.reindex(lookup).array, index=result.index, dtype="string")
        fallback = "TICKER:" + exchange.fillna("UNKNOWN") + ":" + symbol
        ambiguous = pd.Series(lookup.isin(self.ambiguous), index=result.index) & supplied.isna()
        result["symbol"], result["exchange"], result["isin"] = symbol, exchange, supplied.fillna(matched)
        result["security_id"] = ("ISIN:" + result["isin"]).fillna(fallback).mask(ambiguous)
        result["identity_status"] = pd.Series("TICKER_ONLY", index=result.index, dtype="string").mask(
            matched.notna(), "TICKER_MATCH_OBSERVED").mask(supplied.notna(), "SOURCE_ISIN").mask(ambiguous, "AMBIGUOUS")
        return result


def build_references(raw: dict[str, pd.DataFrame], identity: Identity, indexes: dict[str, str]):
    details = identity.attach(raw["securities_details"])
    prices = identity.attach(raw["daily_stock_price"])
    for frame, kind in ((details, "TRADING_DEFINITION"), (prices, "LATEST_TRADE")):
        frame["board"] = series(frame, "boardid").astype("string").fillna("UNSPECIFIED")
        frame["snapshot_kind"] = frame.source.eq("dnse").map({True: kind, False: "COMPANY_PROFILE" if kind == "TRADING_DEFINITION" else "QUOTE_BOARD"})
        frame["snapshot_id"] = (frame.exchange.fillna("UNKNOWN") + "|" + frame.board + "|" + frame.observed_at_utc.astype("string"))
    prices["last_trade_at_utc"] = utc(series(prices, "time")).where(prices.snapshot_kind.eq("LATEST_TRADE"))
    prices["last_price_source"] = pd.to_numeric(series(prices, "matchprice"), errors="coerce")
    prices["last_price_vnd"] = prices.last_price_source.where(prices.source.eq("dnse")) * 1000
    prices["snapshot_date"] = prices.observed_at_utc.dt.tz_convert("Asia/Ho_Chi_Minh").dt.strftime("%Y-%m-%d")

    catalog = identity.attach(raw["securities"])
    identities = identity.evidence.loc[identity.evidence.identity_status.eq("SOURCE_ISIN")].copy()
    master = pd.concat([identities, catalog], ignore_index=True)
    master["_priority"] = master.source.map({"dnse": 2, "vsdc": 0}).fillna(1)
    master = master.sort_values(["_priority", "observed_at_utc"], na_position="first").drop(columns="_priority")
    indices = raw["index_list"].copy()
    indices["source_index_id"] = series(indices, "index_id")
    indices["index_id"] = "INDEX:" + indices.symbol
    indices["exchange"] = indices.exchange.fillna(indices.symbol.map(indexes))
    # The price history contains index identifiers omitted by the built-in group catalogue.
    absent = sorted(set(indexes) - set(indices.symbol.dropna()))
    if absent:
        more = pd.DataFrame({"symbol": absent, "index_id": [f"INDEX:{s}" for s in absent],
                             "exchange": [indexes[s] for s in absent], "source": "market_index_mapping"})
        indices = pd.concat([indices, more], ignore_index=True)
    members = identity.attach(raw["index_components"])
    members["index_id"] = "INDEX:" + members.index_symbol.astype("string")
    members["snapshot_id"] = series(members, "publication_period").astype("string").fillna(members.observed_at_utc.astype("string"))
    members["effective_from"] = pd.to_datetime(series(members, "effective_from"), errors="coerce")
    members["availability_basis"] = "SNAPSHOT_ONLY"
    history = identity.evidence.dropna(subset=["security_id", "exchange", "observed_at_utc"])
    history = history.groupby(["security_id", "symbol", "exchange", "source", "identity_status"], as_index=False).agg(
        first_observed_at_utc=("observed_at_utc", "min"), last_observed_at_utc=("observed_at_utc", "max"),
        first_evidence_date=("evidence_date", "min"), last_evidence_date=("evidence_date", "max"),
        isin=("isin", "last"), evidence_file=("bronze_file", "last"))
    history["valid_from"] = pd.NaT
    history["valid_to"] = pd.NaT
    history["mapping_basis"] = "OBSERVED_ONLY"
    return {
        "securities": (master, ["security_id"]),
        "securities_details": (details, ["security_id", "source", "snapshot_kind", "snapshot_id"]),
        "daily_stock_price": (prices, ["security_id", "source", "snapshot_kind", "snapshot_id"]),
        "index_list": (indices, ["index_id"]),
        "index_components": (members, ["index_id", "security_id", "source", "snapshot_id"]),
        "security_symbol_history": (history, ["security_id", "symbol", "exchange", "source", "identity_status"]),
    }
