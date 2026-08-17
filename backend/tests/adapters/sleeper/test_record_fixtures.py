import json

from tests.conftest import FIXTURE_LEAGUE_ID as LEAGUE


async def test_record_writes_every_fixture_file(tmp_path, sleeper_client):
    from scripts.record_sleeper_fixtures import EXPECTED_FILES, record

    written = await record(LEAGUE, tmp_path, sleeper_client)
    stems = {stem for stem, _ in EXPECTED_FILES}
    assert set(written) == stems
    for stem, _ in EXPECTED_FILES:
        assert (tmp_path / f"{stem}.json").exists()
    assert (tmp_path / "README.md").exists()
    assert LEAGUE in (tmp_path / "README.md").read_text(encoding="utf-8")


async def test_players_slice_is_restricted_to_rostered_players_plus_extras(
    tmp_path, sleeper_client, sleeper_fixture
):
    from scripts.record_sleeper_fixtures import record

    rosters = sleeper_fixture("rosters")
    rostered_ids = {
        pid
        for r in rosters
        for pid in (
            *(r.get("players") or []),
            *(r.get("starters") or []),
            *(r.get("reserve") or []),
            *(r.get("taxi") or []),
        )
        if pid and pid != "0"
    }

    await record(LEAGUE, tmp_path, sleeper_client)
    sliced = json.loads((tmp_path / "players_slice.json").read_text(encoding="utf-8"))

    # Fixture corpus: 23 rostered ids + the 2 free-agent extras baked into
    # players_slice.json ("90", "91") — the mock /players/nfl blob has no others to pick.
    assert len(sliced) == 25
    assert rostered_ids <= sliced.keys()
    assert all(isinstance(v, dict) for v in sliced.values())
