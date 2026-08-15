"""Deterministic bounded source inventory."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from canonical_data.errors import SourceError
from canonical_data.models import Asset


@dataclass(frozen=True)
class SourceObject:
    source_id: str
    url: str
    expected_sha256: str | None = None
    expected_bytes: int | None = None


def expected_5m_market_starts(day: date, cutoff: datetime) -> list[int]:
    if cutoff.tzinfo is None:
        raise SourceError("release cutoff must be timezone-aware")
    midnight = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())
    cutoff_s = int(cutoff.timestamp())
    return [
        midnight + offset
        for offset in range(0, 86_400, 300)
        if midnight + offset < cutoff_s
    ]


def pmxt_hourly_objects(start_ns: int, end_ns: int) -> list[SourceObject]:
    if end_ns <= start_ns:
        raise SourceError("inventory range must be positive")
    start = datetime.fromtimestamp(start_ns // 1_000_000_000, UTC).replace(
        minute=0, second=0, microsecond=0
    )
    end = datetime.fromtimestamp((end_ns - 1) // 1_000_000_000, UTC).replace(
        minute=0, second=0, microsecond=0
    )
    current = start
    result: list[SourceObject] = []
    while current <= end:
        stamp = current.strftime("%Y-%m-%dT%H")
        result.append(
            SourceObject("pmxt_v2", f"https://r2v2.pmxt.dev/polymarket_orderbook_{stamp}.parquet")
        )
        current += timedelta(hours=1)
    return result


def binance_daily_objects(asset: Asset, day: str, kinds: tuple[str, ...]) -> list[SourceObject]:
    symbol = f"{asset.value}USDT"
    market = "um" if asset is Asset.HYPE else "spot"
    root = f"https://data.binance.vision/data/{'futures/um' if market == 'um' else 'spot'}/daily"
    result: list[SourceObject] = []
    for kind in kinds:
        if kind in {"klines", "markPriceKlines", "indexPriceKlines"}:
            filename = f"{symbol}-1m-{day}.zip"
            url = f"{root}/{kind}/{symbol}/1m/{filename}"
        elif kind in {"trades", "aggTrades"}:
            filename = f"{symbol}-{kind}-{day}.zip"
            url = f"{root}/{kind}/{symbol}/{filename}"
        else:
            raise SourceError(f"unsupported Binance kind: {kind}")
        result.append(SourceObject("binance_public", url))
    return result
