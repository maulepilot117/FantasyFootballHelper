"""Behavioural check that the composite same-league FKs actually fire in Postgres."""

import pytest
from sqlalchemy.exc import IntegrityError

from ffh.db.models import League, LeagueTeam, Matchup

pytestmark = pytest.mark.db


def _league(external_id: str) -> League:
    return League(
        platform="sleeper",
        external_id=external_id,
        season=2026,
        num_teams=12,
        scoring_settings={},
        roster_settings={},
        league_type="redraft",
    )


def _two_leagues_one_team_each(db_session) -> tuple[League, LeagueTeam, League, LeagueTeam]:
    a, b = _league("A"), _league("B")
    db_session.add_all([a, b])
    db_session.flush()
    ta = LeagueTeam(league_id=a.league_id, external_id="1")
    tb = LeagueTeam(league_id=b.league_id, external_id="1")
    db_session.add_all([ta, tb])
    db_session.flush()
    return a, ta, b, tb


def test_matchup_home_team_from_other_league_is_rejected(db_session):
    a, _ta, _b, tb = _two_leagues_one_team_each(db_session)
    db_session.add(
        Matchup(league_id=a.league_id, week=1, matchup_no=1, home_team_id=tb.league_team_id)
    )
    with pytest.raises(IntegrityError, match="matchups_home_team_fkey"):
        db_session.flush()


def test_matchup_away_team_from_other_league_is_rejected(db_session):
    a, ta, _b, tb = _two_leagues_one_team_each(db_session)
    db_session.add(
        Matchup(
            league_id=a.league_id,
            week=1,
            matchup_no=1,
            home_team_id=ta.league_team_id,
            away_team_id=tb.league_team_id,
        )
    )
    with pytest.raises(IntegrityError, match="matchups_away_team_fkey"):
        db_session.flush()


def test_matchup_same_league_team_and_bye_succeed(db_session):
    a, ta, _b, _tb = _two_leagues_one_team_each(db_session)
    db_session.add(
        Matchup(league_id=a.league_id, week=1, matchup_no=1, home_team_id=ta.league_team_id)
    )
    db_session.flush()  # away NULL = bye; composite FK does not enforce with a NULL member


def test_leagues_my_team_id_from_other_league_is_rejected(db_session):
    a, _ta, _b, tb = _two_leagues_one_team_each(db_session)
    a.my_team_id = tb.league_team_id
    with pytest.raises(IntegrityError, match="leagues_my_team_fkey"):
        db_session.flush()


def test_leagues_my_team_id_from_same_league_succeeds(db_session):
    a, ta, _b, _tb = _two_leagues_one_team_each(db_session)
    a.my_team_id = ta.league_team_id
    db_session.flush()
