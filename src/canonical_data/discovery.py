"""Official Gamma discovery and authority binding."""

from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from decimal import Decimal
from typing import Any, cast

from canonical_data.audit import canonical_json_bytes
from canonical_data.errors import IdentityError, UnresolvedMarketError
from canonical_data.httpclient import USER_AGENT
from canonical_data.models import Asset, Market, Outcome, QualityTier

SLUG = re.compile(r"^(doge|bnb|hype)-updown-5m-([0-9]{10})$")
CONDITION = re.compile(r"^0x[0-9a-f]{64}$")
ASSET_STREAMS = {
    Asset.DOGE: "https://data.chain.link/streams/doge-usd",
    Asset.BNB: "https://data.chain.link/streams/bnb-usd",
    Asset.HYPE: "https://data.chain.link/streams/hype-usd",
}
ASSET_TWAP_STREAMS = {
    Asset.DOGE: "https://data.chain.link/streams/doge-usd-twap-30s-streams",
    Asset.BNB: "https://data.chain.link/streams/bnb-usd-twap-30s-streams",
    Asset.HYPE: "https://data.chain.link/streams/hype-usd-twap-30s-streams",
}
ASSET_RULE_NAMES = {
    Asset.DOGE: ("doge", "dogecoin"),
    Asset.BNB: ("bnb", "binance coin"),
    Asset.HYPE: ("hype", "hyperliquid"),
}

FetchPayload = Callable[[str, int], bytes]


def _bounded_fetch(url: str, max_bytes: int) -> bytes:
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        length = response.headers.get("Content-Length")
        if length is not None and int(length) > max_bytes:
            raise IdentityError("Gamma payload exceeds configured bound")
        payload = cast(bytes, response.read(max_bytes + 1))
    if len(payload) > max_bytes:
        raise IdentityError("Gamma payload exceeds configured bound")
    return payload


class GammaClient:
    def __init__(self, fetch: FetchPayload = _bounded_fetch, max_payload_bytes: int = 1_000_000):
        self.fetch = fetch
        self.max_payload_bytes = max_payload_bytes

    def fetch_slug_payload(self, asset: Asset, market_start_s: int) -> tuple[bytes, str]:
        slug = f"{asset.value.lower()}-updown-5m-{market_start_s}"
        query = urllib.parse.urlencode({"slug": slug})
        url = f"https://gamma-api.polymarket.com/events?{query}"
        payload = self.fetch(url, self.max_payload_bytes)
        return payload, url

    def fetch_market(self, asset: Asset, market_start_s: int) -> tuple[Market, bytes, str]:
        payload, url = self.fetch_slug_payload(asset, market_start_s)
        markets = discover([payload])
        if len(markets) != 1:
            raise IdentityError("Gamma slug lookup did not return exactly one official market")
        return markets[0], payload, url


def _json_list(value: Any, name: str) -> list[Any]:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, list):
        raise IdentityError(f"{name} must be a list")
    return parsed


def _official_outcome(prices: list[Any]) -> Outcome:
    values = [Decimal(str(item)) for item in prices]
    if values == [Decimal(1), Decimal(0)]:
        return Outcome.UP
    if values == [Decimal(0), Decimal(1)]:
        return Outcome.DOWN
    if values == [Decimal("0.5"), Decimal("0.5")]:
        return Outcome.SPLIT
    raise IdentityError("closed market lacks an unambiguous official outcome")


def _rules_bind_stream(asset: Asset, rules: str, source_url: object) -> str:
    """Bind the exact stream from controlling rules plus the Gamma source field."""
    spot = ASSET_STREAMS[asset]
    twap = ASSET_TWAP_STREAMS[asset]
    allowed = (spot, twap)
    declared = source_url.strip().rstrip("/") if isinstance(source_url, str) else ""
    rules_lower = rules.lower()
    rules_name_stream = (
        "chainlink" in rules_lower and f"{asset.value.lower()}/usd" in rules_lower
    )
    rules_streams = tuple(stream for stream in allowed if stream in rules or f"{stream}/" in rules)
    rules_asset_identity = any(
        re.search(rf"(?<![a-z0-9]){re.escape(name)}(?![a-z0-9])", rules_lower)
        for name in ASSET_RULE_NAMES[asset]
    )
    # Gamma's dedicated resolutionSource field is authoritative when it is the exact
    # frozen stream and the controlling prose independently names the bound asset.
    if declared == spot and (spot in rules_streams or rules_name_stream or rules_asset_identity):
        return spot
    if (
        declared == twap
        and twap in rules_streams
        and "chainlink" in rules_lower
        and "twap" in rules_lower
        and rules_asset_identity
    ):
        return twap
    # The per-market rules are controlling when Gamma leaves its redundant summary field blank.
    if not declared and len(rules_streams) == 1:
        rules_stream = rules_streams[0]
        if rules_stream == spot or (
            "chainlink" in rules_lower and "twap" in rules_lower and rules_asset_identity
        ):
            return rules_stream
    rules_digest = hashlib.sha256(rules.encode()).hexdigest()
    excerpt = " ".join(rules.split())[:500]
    raise IdentityError(
        "rules do not bind the frozen Chainlink stream: "
        f"asset={asset.value} resolutionSource={declared!r} "
        f"rules_sha256={rules_digest} rules_excerpt={excerpt!r}"
    )


def bind_gamma_market(event: dict[str, Any], retrieved_payload: bytes) -> Market:
    markets = event.get("markets")
    if not isinstance(markets, list) or len(markets) != 1 or not isinstance(markets[0], dict):
        raise IdentityError("event must contain exactly one market")
    raw = markets[0]
    slug = raw.get("slug")
    match = SLUG.fullmatch(slug) if isinstance(slug, str) else None
    if match is None:
        raise IdentityError("unsupported market slug")
    slug = cast(str, slug)
    asset = Asset(match.group(1).upper())
    start_ns = int(match.group(2)) * 1_000_000_000
    end_ns = start_ns + 300_000_000_000
    outcomes = [str(value).upper() for value in _json_list(raw.get("outcomes"), "outcomes")]
    tokens = [str(value) for value in _json_list(raw.get("clobTokenIds"), "clobTokenIds")]
    if (
        outcomes != ["UP", "DOWN"]
        or len(tokens) != 2
        or not all(token.isdigit() for token in tokens)
    ):
        raise IdentityError("outcome/token mapping must be exactly Up,Down")
    condition_id = raw.get("conditionId")
    if not isinstance(condition_id, str) or CONDITION.fullmatch(condition_id) is None:
        raise IdentityError("invalid condition id")
    rules = raw.get("description")
    source_url = raw.get("resolutionSource")
    if not isinstance(rules, str) or not rules.strip():
        raise IdentityError("rules are missing")
    bound_source_url = _rules_bind_stream(asset, rules, source_url)
    if "greater than or equal" not in rules.lower() or "otherwise" not in rules.lower():
        raise IdentityError("rules do not prove Up/Down comparison semantics")
    event_id = str(event.get("id", ""))
    market_id = str(raw.get("id", ""))
    if not event_id or not market_id:
        raise IdentityError("official ids are missing")
    if raw.get("closed") is not True:
        raise UnresolvedMarketError(slug, market_id, condition_id)
    outcome = _official_outcome(_json_list(raw.get("outcomePrices"), "outcomePrices"))
    evidence_digest = hashlib.sha256(retrieved_payload).hexdigest()
    rules_digest = hashlib.sha256(rules.encode()).hexdigest()
    return Market(
        asset=asset,
        event_id=event_id,
        market_id=market_id,
        condition_id=condition_id,
        token_up=tokens[0],
        token_down=tokens[1],
        market_start_ns=start_ns,
        market_end_ns=end_ns,
        rules_text_sha256=rules_digest,
        resolution_source_url=bound_source_url,
        official_outcome=outcome,
        official_resolution_ts_ns=None,
        quality_tier=QualityTier.TIER_A,
        evidence_sha256=evidence_digest,
    )


def discover(payloads: Iterable[bytes]) -> list[Market]:
    markets: list[Market] = []
    seen: dict[str, bytes] = {}
    for payload in payloads:
        decoded = json.loads(payload)
        events = decoded if isinstance(decoded, list) else [decoded]
        for event in events:
            if not isinstance(event, dict):
                raise IdentityError("Gamma response contains a non-object")
            canonical = canonical_json_bytes(event)
            market = bind_gamma_market(event, payload)
            previous = seen.get(market.condition_id)
            if previous is not None and previous != canonical:
                raise IdentityError("conflicting official market identity")
            seen[market.condition_id] = canonical
            markets.append(market)
    return sorted(
        {market.condition_id: market for market in markets}.values(),
        key=lambda item: item.condition_id,
    )
