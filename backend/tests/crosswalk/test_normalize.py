import pytest

from ffh.crosswalk.normalize import (
    ALIASES,
    FANTASY_POSITIONS,
    TEAMS,
    dst_full_name,
    normalize_dst,
    normalize_name,
    normalize_position,
    normalize_team,
)

# Every example from DATABASE.md §3 is here (suffixes, D.J./DJ/D J, Ja'Marr, Amon-Ra,
# Robby/Robert, Cam/Cameron) plus the tricky real names from nflverse/DynastyProcess.
NAME_CASES = [
    ("Odell Beckham Jr.", "odell beckham"),
    ("Odell Beckham Jr", "odell beckham"),
    ("Odell Beckham", "odell beckham"),
    ("Kenneth Walker III", "kenneth walker"),
    ("KENNETH WALKER III", "kenneth walker"),
    ("Kenneth Walker", "kenneth walker"),
    ("Ken Walker", "kenneth walker"),
    ("Amon-Ra St. Brown", "amonra st brown"),
    ("Amon Ra St Brown", "amon ra st brown"),
    ("Ja'Marr Chase", "jamarr chase"),
    ("Ja’Marr Chase", "jamarr chase"),  # curly apostrophe  # noqa: RUF001
    ("Jamarr Chase", "jamarr chase"),
    ("D.J. Moore", "dj moore"),
    ("DJ Moore", "dj moore"),
    ("D J Moore", "dj moore"),
    ("A.J. Brown", "aj brown"),
    ("  A.J.   Brown ", "aj brown"),
    ("T.J. Hockenson", "tj hockenson"),
    ("Marvin Harrison Jr.", "marvin harrison"),
    ("Michael Pittman Jr.", "michael pittman"),
    ("Mike Pittman", "michael pittman"),
    ("Brian Robinson Jr.", "brian robinson"),
    ("Travis Etienne Jr.", "travis etienne"),
    ("Robby Anderson", "robert anderson"),
    ("Robbie Anderson", "robert anderson"),
    ("Robert Anderson", "robert anderson"),
    ("Cam Akers", "cameron akers"),
    ("Cameron Akers", "cameron akers"),
    ("Mitch Trubisky", "mitchell trubisky"),
    ("Mitchell Trubisky", "mitchell trubisky"),
    ("Patrick Mahomes II", "patrick mahomes"),
    ("Patrick Mahomes", "patrick mahomes"),
    ("Pat Mahomes", "patrick mahomes"),
    ("Josh Allen", "joshua allen"),
    ("Joshua Allen", "joshua allen"),
    ("Matt Stafford", "matthew stafford"),
    ("Chris Olave", "christopher olave"),
    ("Nick Chubb", "nicholas chubb"),
    ("Will Levis", "william levis"),
    ("Kenny Pickett", "kenneth pickett"),
    ("Tony Pollard", "anthony pollard"),
    ("Dan Campbell", "daniel campbell"),
    ("Gabe Davis", "gabriel davis"),
    ("Ulysses Bentley IV", "ulysses bentley"),
    ("Vinny Anthony II", "vinny anthony"),
    ("Larry Fitzgerald Sr.", "larry fitzgerald"),
    ("Kirk Cousins  ", "kirk cousins"),
    ("De'Von Achane", "devon achane"),
    ("Ray-Ray McCloud", "rayray mccloud"),
    ("Ja'Quinden Jackson", "jaquinden jackson"),
    ("Chig Okonkwo", "chig okonkwo"),
    ("Jr.", "jr"),  # a bare suffix is left alone — never return ""
    ("", ""),
    # Non-ASCII letters are FOLDED, not destroyed: `[^a-z0-9 ]` used to run after
    # .lower() and turn "Andrés Peña" into "andr s pe a" — an accented spelling could
    # then never match the ASCII spelling another source publishes.
    ("Andrés Peña", "andres pena"),
    ("Andres Pena", "andres pena"),
    ("ANDRÉS PEÑA", "andres pena"),
    ("Björn Söderström", "bjorn soderstrom"),
    ("José Ángel Muñoz", "jose angel munoz"),
    ("Kalié Duplechain", "kalie duplechain"),
]


@pytest.mark.parametrize(("raw", "expected"), NAME_CASES)
def test_normalize_name(raw: str, expected: str) -> None:
    assert normalize_name(raw) == expected


def test_accented_names_do_not_collapse_together() -> None:
    """Folding must not merge two different people: the old `[^a-z0-9 ]` substitution
    turned every accented letter into a space, so distinct names could share a key."""
    assert normalize_name("Peña") != normalize_name("Pena Rodriguez")
    assert len({normalize_name(n) for n in ("Andrés Peña", "Andrés Peñaloza")}) == 2
    # …and the folded form is exactly what the ASCII spelling produces.
    assert normalize_name("Andrés Peña") == normalize_name("Andres Pena") == "andres pena"


def test_normalize_name_is_idempotent() -> None:
    for raw, _ in NAME_CASES:
        once = normalize_name(raw)
        assert normalize_name(once) == once


def test_alias_table_has_required_entries() -> None:
    required = {
        "robby": "robert",
        "rob": "robert",
        "cam": "cameron",
        "mitch": "mitchell",
        "josh": "joshua",
        "mike": "michael",
        "matt": "matthew",
        "chris": "christopher",
        "nick": "nicholas",
        "pat": "patrick",
        "will": "william",
        "ken": "kenneth",
        "tony": "anthony",
        "dan": "daniel",
    }
    for k, v in required.items():
        assert ALIASES[k] == v
    assert len(ALIASES) >= 12
    # keys and values are already-normalized single tokens
    for k, v in ALIASES.items():
        assert k == k.lower() and " " not in k and v == v.lower() and " " not in v


DST_CASES = [
    ("KC", "kc dst"),
    ("KC DST", "kc dst"),
    ("KC D/ST", "kc dst"),
    ("Chiefs D/ST", "kc dst"),
    ("Chiefs DST", "kc dst"),
    ("Chiefs", "kc dst"),
    ("Kansas City", "kc dst"),
    ("Kansas City Chiefs", "kc dst"),
    ("Kansas City Chiefs DST", "kc dst"),
    ("Kansas City Chiefs Defense", "kc dst"),
    ("KAN", "kc dst"),  # PFR
    ("CRD", "ari dst"),  # PFR — the five abbreviations that look nothing like the team
    ("RAV", "bal dst"),  # PFR
    ("HTX", "hou dst"),  # PFR
    ("CLT", "ind dst"),  # PFR
    ("RAI", "lv dst"),  # PFR
    ("KCC", "kc dst"),  # MFL / DynastyProcess
    ("kc dst", "kc dst"),  # canonical form is a fixed point
    ("Los Angeles Rams", "la dst"),
    ("LA", "la dst"),  # nflverse abbreviation for the Rams
    ("LAR", "la dst"),  # Sleeper / ESPN / Yahoo
    ("Rams", "la dst"),
    ("St. Louis Rams", "la dst"),
    ("LA Chargers", "lac dst"),
    ("Chargers", "lac dst"),
    ("SD", "lac dst"),
    ("Las Vegas Raiders", "lv dst"),
    ("OAK", "lv dst"),
    ("LVR", "lv dst"),
    ("Washington Football Team", "was dst"),
    ("WSH", "was dst"),  # ESPN
    ("Commanders", "was dst"),
    ("49ers", "sf dst"),
    ("San Francisco 49ers D/ST", "sf dst"),
    ("Niners", "sf dst"),
    ("SFO", "sf dst"),
    ("Bucs", "tb dst"),
    ("TBB", "tb dst"),
    ("Tampa Bay Buccaneers", "tb dst"),
    ("GNB", "gb dst"),
    ("GBP", "gb dst"),
    ("Packers", "gb dst"),
    ("JAC", "jax dst"),
    ("Jaguars", "jax dst"),
    ("NY Giants", "nyg dst"),
    ("New York Giants", "nyg dst"),
    ("New York Jets", "nyj dst"),
    ("Jets D/ST", "nyj dst"),
    ("NWE", "ne dst"),
    ("Patriots", "ne dst"),
    ("NOR", "no dst"),
    ("Saints", "no dst"),
    ("New York", None),  # ambiguous city
    ("Los Angeles", None),  # ambiguous city
    ("Josh Allen", None),
    ("FA", None),
    ("FA*", None),
    ("", None),
    (None, None),
]


@pytest.mark.parametrize(("raw", "expected"), DST_CASES)
def test_normalize_dst(raw: str | None, expected: str | None) -> None:
    assert normalize_dst(raw) == expected


@pytest.mark.parametrize(("raw", "expected"), DST_CASES)
def test_normalize_team_matches_dst_table(raw: str | None, expected: str | None) -> None:
    abbr = normalize_team(raw)
    assert abbr == (expected.split()[0].upper() if expected else None)


def test_teams_table_is_complete_and_unique() -> None:
    abbrs = [t[0] for t in TEAMS]
    assert len(abbrs) == 32 and len(set(abbrs)) == 32
    assert set(abbrs) == {
        "ARI",
        "ATL",
        "BAL",
        "BUF",
        "CAR",
        "CHI",
        "CIN",
        "CLE",
        "DAL",
        "DEN",
        "DET",
        "GB",
        "HOU",
        "IND",
        "JAX",
        "KC",
        "LA",
        "LAC",
        "LV",
        "MIA",
        "MIN",
        "NE",
        "NO",
        "NYG",
        "NYJ",
        "PHI",
        "PIT",
        "SEA",
        "SF",
        "TB",
        "TEN",
        "WAS",
    }
    for abbr, _city, _nick, _aliases in TEAMS:
        assert normalize_team(abbr) == abbr
        assert normalize_dst(abbr) == f"{abbr.lower()} dst"


def test_dst_full_name() -> None:
    assert dst_full_name("KC") == "Kansas City Chiefs DST"
    assert dst_full_name("LA") == "Los Angeles Rams DST"


POSITION_CASES = [
    ("QB", "QB"),
    ("qb", "QB"),
    (" rb ", "RB"),
    ("WR", "WR"),
    ("TE", "TE"),
    ("K", "K"),
    ("PK", "K"),  # DynastyProcess kickers are PK
    ("FB", "RB"),
    ("HB", "RB"),  # fullbacks live in the RB pool
    ("DST", "DST"),
    ("DEF", "DST"),
    ("D/ST", "DST"),
    ("D ST", "DST"),
    ("OL", None),
    ("CB", None),
    ("P", None),
    ("LB", None),
    ("XX", None),
    ("", None),
    (None, None),
]


@pytest.mark.parametrize(("raw", "expected"), POSITION_CASES)
def test_normalize_position(raw: str | None, expected: str | None) -> None:
    assert normalize_position(raw) == expected
    if expected is not None:
        assert expected in FANTASY_POSITIONS
