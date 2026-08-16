"""ORM models. Import this package to register every table on Base.metadata."""

from ffh.db.models.crosswalk import CrosswalkUnmatched
from ffh.db.models.decisions import AiDebate, IngestRun, Recommendation
from ffh.db.models.draft import Adp, Draft, DraftPick
from ffh.db.models.league import League, LeagueTeam, Matchup, RosterSlot, Transaction
from ffh.db.models.projections import (
    GENERIC_LEAGUE_ID,
    PlayerInjuryStatus,
    PlayerWeekActual,
    Projection,
    ProjectionCorrelation,
)
from ffh.db.models.reference import (
    Game,
    GameWeatherForecast,
    NflTeam,
    Player,
    PlayerExternalId,
    Stadium,
)

__all__ = [
    "GENERIC_LEAGUE_ID",
    "Adp",
    "AiDebate",
    "CrosswalkUnmatched",
    "Draft",
    "DraftPick",
    "Game",
    "GameWeatherForecast",
    "IngestRun",
    "League",
    "LeagueTeam",
    "Matchup",
    "NflTeam",
    "Player",
    "PlayerExternalId",
    "PlayerInjuryStatus",
    "PlayerWeekActual",
    "Projection",
    "ProjectionCorrelation",
    "Recommendation",
    "RosterSlot",
    "Stadium",
    "Transaction",
]
