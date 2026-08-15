"""Frozen quality and exclusion classification."""

from __future__ import annotations

from canonical_data.models import Exclusion, ExclusionReason, Market, QualityTier

TIER_A_START = 1_776_106_800_000_000_000  # 2026-04-13T19:00:00Z
RELEASE_START = 1_775_347_200_000_000_000  # 2026-04-05T00:00:00Z
TIER_B_END = 1_779_148_799_999_999_999  # 2026-05-18T23:59:59.999999999Z


def classify(
    market: Market, has_pmxt: bool, has_kacho: bool, gaps: list[tuple[int, int]]
) -> tuple[QualityTier, Exclusion | None]:
    if market.timeframe != "5m":
        return _excluded(market, ExclusionReason.UNSUPPORTED_TIMEFRAME, "only 5m is frozen")
    if market.market_start_ns < RELEASE_START:
        return _excluded(market, ExclusionReason.OUT_OF_SCOPE_DATE, "before frozen release start")
    if market.official_outcome.value == "UNRESOLVED":
        return _excluded(market, ExclusionReason.UNRESOLVED_MARKET, "official outcome unresolved")
    if market.market_start_ns >= TIER_A_START and has_pmxt and not gaps:
        return QualityTier.TIER_A, None
    if market.market_start_ns <= TIER_B_END and has_kacho:
        return QualityTier.TIER_B, None
    reason = (
        ExclusionReason.SOURCE_GAP
        if gaps or not (has_pmxt or has_kacho)
        else ExclusionReason.NO_INITIAL_SNAPSHOT
    )
    return _excluded(market, reason, "source fidelity cannot satisfy a frozen tier")


def _excluded(
    market: Market, reason: ExclusionReason, detail: str
) -> tuple[QualityTier, Exclusion]:
    return QualityTier.EXCLUDED, Exclusion(market.market_id, reason, detail)
