"""Land Sleeper ``GET /players/nfl`` to the Parquet lake, at most once a day.

The response is 14.6 MB and a dict keyed by Sleeper player id (12,219 entries, 32 of them
team defenses with no ``full_name`` and no ``gsis_id``). Sleeper returns an ``ETag`` but
IGNORES ``If-None-Match`` (verified 2026-08-16: sending it still yields 200 + full body), so
``ffh.ingest.http.get_bytes``' conditional-GET path cannot detect freshness here. Instead
freshness is a sha256 of the body stored in ``ingest_runs.source_etag`` as ``sha256:<hex>``.

The once-a-day guarantee is ③'s: ``IngestJob.run`` writes through ``write_parquet``, which
refuses to overwrite today's ``scrape_date=`` partition (``PartitionExistsError`` ->
``skipped``, still recorded in ``ingest_runs``). No ``run()`` override lives here.

Every column lands as Utf8: ``espn_id``/``yahoo_id`` arrive as ints, but the crosswalk joins
them as text and a mixed-type column would silently coerce.

Import direction is ingest -> adapters: this module consumes the adapter's ``RawPlayer``,
``player_ref`` and the catalog's ``REQUIRED_COLUMNS`` (the ONE player-column contract).
Sleeper's API is licensed for non-commercial use only.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, ClassVar

import polars as pl
import structlog

from ffh.adapters.sleeper.adapter import player_ref
from ffh.adapters.sleeper.catalog import REQUIRED_COLUMNS
from ffh.adapters.sleeper.models import RawPlayer
from ffh.config import get_settings
from ffh.ingest.base import Fetched, IngestJob, IngestValidationError, NotModified, register
from ffh.ingest.http import FetchResult, get_bytes, make_client
from ffh.ingest.lake import scrape_date

log = structlog.get_logger(__name__)

PLAYERS_PATH = "/players/nfl"

#: The lake column order. The catalog's REQUIRED_COLUMNS lead, verbatim.
PLAYER_COLUMNS: tuple[str, ...] = (
    *REQUIRED_COLUMNS,
    "first_name",
    "last_name",
    "fantasy_positions",
    "status",
    "active",
    "injury_status",
    "gsis_id",
    "espn_id",
    "yahoo_id",
    "rotowire_id",
    "fantasy_data_id",
    "sportradar_id",
    "birth_date",
    "college",
    "years_exp",
    "number",
    "depth_chart_order",
    "depth_chart_position",
    "search_rank",
)


def _s(value: Any) -> str | None:
    """Stringify a scalar for the all-Utf8 frame; bools become 'true'/'false'."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _row(raw: RawPlayer) -> dict[str, str | None]:
    ref = player_ref(raw)
    return {
        "player_id": ref.external_id,
        "name": ref.name,
        "position": ref.position,
        "team": ref.team,
        "first_name": raw.first_name,
        "last_name": raw.last_name,
        "fantasy_positions": "|".join(raw.fantasy_positions) or None,
        "status": raw.status,
        "active": _s(raw.active),
        "injury_status": raw.injury_status,
        # Normalized by player_ref (stray leading space stripped, "" -> None) so the lake
        # value and the catalog's PlayerRef.gsis_id agree.
        "gsis_id": ref.gsis_id,
        "espn_id": _s(raw.espn_id),
        "yahoo_id": _s(raw.yahoo_id),
        "rotowire_id": _s(raw.rotowire_id),
        "fantasy_data_id": _s(raw.fantasy_data_id),
        "sportradar_id": raw.sportradar_id,
        "birth_date": raw.birth_date,
        "college": raw.college,
        "years_exp": _s(raw.years_exp),
        "number": _s(raw.number),
        "depth_chart_order": _s(raw.depth_chart_order),
        "depth_chart_position": raw.depth_chart_position,
        "search_rank": _s(raw.search_rank),
    }


def players_to_frame(payload: dict[str, dict[str, Any]]) -> pl.DataFrame:
    """The ``/players/nfl`` dict -> one all-Utf8 row per entry, in ``PLAYER_COLUMNS`` order.

    Row-count invariants only; the duplicate/null ``player_id`` checks belong to
    ``SleeperPlayersJob.validate`` so they surface as ``IngestValidationError``.
    """
    if not payload:
        raise ValueError(f"GET {PLAYERS_PATH} returned an empty payload")
    rows = [_row(RawPlayer.model_validate(entry)) for entry in payload.values()]
    df = pl.DataFrame(rows, schema={c: pl.Utf8 for c in PLAYER_COLUMNS})
    if df.height != len(payload):
        raise ValueError(f"lost rows building the frame: {len(payload)} in, {df.height} out")
    return df


@register
class SleeperPlayersJob(IngestJob):
    """Registered as ``sleeper_players``.

    Sync ``fetch`` on purpose: ③'s ``IngestJob.run`` calls ``self.fetch(etag)`` directly.
    This module never touches the async adapter client — the blob is not on the request path.
    """

    name: ClassVar[str] = "sleeper_players"
    source: ClassVar[str] = "sleeper"
    asset: ClassVar[str] = "players"
    # The crosswalk cannot work without these; ③'s base validate() checks them first.
    REQUIRED_COLUMNS: ClassVar[frozenset[str]] = frozenset(REQUIRED_COLUMNS)

    def partition(self) -> dict[str, str]:
        # ③'s UTC clock — the same key every other lake partition uses.
        return {"scrape_date": scrape_date()}

    def fetch(self, etag: str | None) -> FetchResult:
        """Unconditional GET through the shared ingest client, then compare body digests.

        ``etag`` is our own ``sha256:<hex>`` from the last successful run, never sent to
        Sleeper: the server ignores ``If-None-Match`` (see module docstring), so the
        conditional-GET branch of ``get_bytes`` is unusable and we always pass ``None``.
        The shared client still gives us the ffh User-Agent, tenacity retry on 429/5xx and
        ``Retry-After`` handling.
        """
        url = get_settings().sleeper_base_url.rstrip("/") + PLAYERS_PATH
        with make_client(timeout=60.0) as client:
            result = get_bytes(client, url, None)
        if not isinstance(result, Fetched):
            return result  # NotFound -> base maps to failed (skip_on_404 is False)
        digest = f"sha256:{hashlib.sha256(result.content).hexdigest()}"
        if etag is not None and etag == digest:
            log.info("ingest.sleeper_players.unchanged", digest=digest)
            return NotModified(etag=digest)
        return Fetched(content=result.content, etag=digest, mtime=datetime.now(UTC))

    def parse(self, content: bytes) -> pl.DataFrame:
        return players_to_frame(json.loads(content))

    def validate(self, df: pl.DataFrame) -> None:
        # ③'s contract: raise IngestValidationError, never a bare assert. The base checks
        # REQUIRED_COLUMNS and the empty frame; the two extra checks are ours.
        super().validate(df)
        if df["player_id"].null_count() != 0:
            raise IngestValidationError(f"{type(self).name}: null player_id")
        if df["player_id"].n_unique() != df.height:
            raise IngestValidationError(f"{type(self).name}: duplicate player_id")
