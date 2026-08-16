"""ORM models. Import this package to register every table on Base.metadata."""

from ffh.db.models.crosswalk import CrosswalkUnmatched
from ffh.db.models.draft import Adp, Draft, DraftPick
from ffh.db.models.league import League, LeagueTeam, Matchup, RosterSlot, Transaction
from ffh.db.models.reference import (
    Game,
    GameWeatherForecast,
    NflTeam,
    Player,
    PlayerExternalId,
    Stadium,
)

__all__ = [
    "Adp",
    "CrosswalkUnmatched",
    "Draft",
    "DraftPick",
    "Game",
    "GameWeatherForecast",
    "League",
    "LeagueTeam",
    "Matchup",
    "NflTeam",
    "Player",
    "PlayerExternalId",
    "RosterSlot",
    "Stadium",
    "Transaction",
]
