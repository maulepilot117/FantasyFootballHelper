"""Coverage report for the crosswalk: what is mapped, what needs review, what is unmatched."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

# The SQL usability rule below MUST mirror ffh.crosswalk.resolve.is_usable:
# player_external_ids.confidence is Postgres REAL (float4), so a stored 0.9 reads back
# as 0.899999988… — the shared epsilon keeps the report and the ladder in agreement
# about which rows are usable versus awaiting review.
from ffh.crosswalk.resolve import CONFIDENCE_EPSILON, USABLE_CONFIDENCE
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId


@dataclass(frozen=True)
class UnverifiedRow:
    source: str
    external_id: str
    player_id: uuid.UUID
    full_name: str
    position: str
    confidence: float
    created_at: datetime


@dataclass(frozen=True)
class UnmatchedRow:
    source: str
    external_id: str
    raw_name: str | None
    raw_position: str | None
    raw_team: str | None
    first_seen: datetime
    last_seen: datetime


@dataclass(frozen=True)
class CoverageReport:
    players_total: int
    players_by_position: dict[str, int]
    ids_by_source: dict[str, int]
    ids_by_source_method: dict[str, dict[str, int]]
    unverified_low_confidence: tuple[UnverifiedRow, ...]
    unmatched: tuple[UnmatchedRow, ...]

    @property
    def ok(self) -> bool:
        return not self.unverified_low_confidence and not self.unmatched

    def to_dict(self) -> dict[str, object]:
        d = asdict(self)
        d["ok"] = self.ok
        # Sort the count maps: SQL GROUP BY order is unspecified, and `--json` output
        # must be deterministic across identical databases (render() already sorts).
        d["players_by_position"] = dict(sorted(self.players_by_position.items()))
        d["ids_by_source"] = dict(sorted(self.ids_by_source.items()))
        d["ids_by_source_method"] = {
            source: dict(sorted(methods.items()))
            for source, methods in sorted(self.ids_by_source_method.items())
        }
        for key in ("unverified_low_confidence", "unmatched"):
            d[key] = [
                {k: (str(v) if isinstance(v, uuid.UUID | datetime) else v) for k, v in row.items()}
                for row in d[key]
            ]
        return d

    def render(self) -> str:
        by_position = " ".join(f"{p}={n}" for p, n in sorted(self.players_by_position.items()))
        lines = [
            f"players: {self.players_total} {by_position}".rstrip(),
            "external ids by source / method:",
        ]
        for source in sorted(self.ids_by_source):
            methods = ", ".join(
                f"{m}={n}" for m, n in sorted(self.ids_by_source_method[source].items())
            )
            lines.append(f"  {source:<12}{self.ids_by_source[source]:>7}  ({methods})")
        lines.append(f"unverified low-confidence: {len(self.unverified_low_confidence)}")
        for r in self.unverified_low_confidence:
            lines.append(
                f"  {r.source}:{r.external_id} -> {r.full_name} ({r.position}) conf={r.confidence:.2f}"
            )
        lines.append(f"unmatched: {len(self.unmatched)}")
        for r in self.unmatched:
            lines.append(
                f"  {r.source}:{r.external_id} {r.raw_name!r} {r.raw_position} {r.raw_team} "
                f"first={r.first_seen:%Y-%m-%d} last={r.last_seen:%Y-%m-%d}"
            )
        lines.append("OK" if self.ok else "ATTENTION REQUIRED")
        return "\n".join(lines)


def coverage_report(session: Session) -> CoverageReport:
    players_total = session.scalar(select(func.count()).select_from(Player)) or 0
    players_by_position = {
        pos: n
        for pos, n in session.execute(
            select(Player.position, func.count()).group_by(Player.position)
        )
    }
    by_source_method: dict[str, dict[str, int]] = {}
    for source, method, n in session.execute(
        select(PlayerExternalId.source, PlayerExternalId.match_method, func.count()).group_by(
            PlayerExternalId.source, PlayerExternalId.match_method
        )
    ):
        by_source_method.setdefault(source, {})[method] = n
    ids_by_source = {s: sum(m.values()) for s, m in by_source_method.items()}

    unverified = tuple(
        UnverifiedRow(
            e.source,
            e.external_id,
            e.player_id,
            p.full_name,
            p.position,
            float(e.confidence),
            e.created_at,
        )
        for e, p in session.execute(
            select(PlayerExternalId, Player)
            .join(Player, Player.player_id == PlayerExternalId.player_id)
            .where(
                PlayerExternalId.confidence < USABLE_CONFIDENCE - CONFIDENCE_EPSILON,
                PlayerExternalId.verified_at.is_(None),
            )
            .order_by(PlayerExternalId.source, PlayerExternalId.external_id)
        )
    )
    unmatched = tuple(
        UnmatchedRow(
            u.source,
            u.external_id,
            u.raw_name,
            u.raw_position,
            u.raw_team,
            u.first_seen,
            u.last_seen,
        )
        # `resolved = false` alone: the bookkeeping is maintained at the source — every
        # mapping-creation path closes its queue entry (resolve.close_unmatched) — so an
        # open row genuinely needs attention. In particular the upgrade-conflict state
        # (disputed mapping in player_external_ids AND an open queue row for the same
        # key) MUST surface here; a NOT EXISTS against the mapping table would hide it.
        for u in session.scalars(
            select(CrosswalkUnmatched)
            .where(CrosswalkUnmatched.resolved.is_(False))
            .order_by(CrosswalkUnmatched.source, CrosswalkUnmatched.external_id)
        )
    )
    return CoverageReport(
        players_total=players_total,
        players_by_position=players_by_position,
        ids_by_source=ids_by_source,
        ids_by_source_method=by_source_method,
        unverified_low_confidence=unverified,
        unmatched=unmatched,
    )
