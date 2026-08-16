"""ORM models. Import this package to register every table on Base.metadata."""

from ffh.db.models.crosswalk import CrosswalkUnmatched
from ffh.db.models.reference import (
    Game,
    GameWeatherForecast,
    NflTeam,
    Player,
    PlayerExternalId,
    Stadium,
)

__all__ = [
    "CrosswalkUnmatched",
    "Game",
    "GameWeatherForecast",
    "NflTeam",
    "Player",
    "PlayerExternalId",
    "Stadium",
]
