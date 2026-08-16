import pytest
from sqlalchemy import select

from ffh.db.models import NflTeam

pytestmark = pytest.mark.db


def test_db_session_rolls_back_between_tests_1(db_session):
    db_session.add(NflTeam(team_abbr="ZZZ", full_name="Test", conference="AFC", division="North"))
    db_session.flush()
    assert db_session.scalar(select(NflTeam).where(NflTeam.team_abbr == "ZZZ")) is not None


def test_db_session_rolls_back_between_tests_2(db_session):
    assert db_session.scalar(select(NflTeam).where(NflTeam.team_abbr == "ZZZ")) is None
