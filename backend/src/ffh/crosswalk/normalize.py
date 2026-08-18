"""Name / team / DST / position normalization for the crosswalk (DATABASE.md §3).

Pure functions. No I/O, no ``ffh.db``, no ``ffh.ingest``. Both sides of every match
(registry rows and incoming ids) go through the same functions, so what matters is that
the output is deterministic — not that it looks like the "real" name.
"""

from __future__ import annotations

import re
import unicodedata

# ---------------------------------------------------------------------------
# Person names
# ---------------------------------------------------------------------------

SUFFIXES: frozenset[str] = frozenset({"jr", "sr", "ii", "iii", "iv", "v"})

# Applied to the FIRST token only, after suffix stripping. Keys/values are normalized
# single tokens. Documented in DATABASE.md §3 ("alias table location").
ALIASES: dict[str, str] = {
    "robby": "robert",
    "robbie": "robert",
    "rob": "robert",
    "bob": "robert",
    "bobby": "robert",
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
    "kenny": "kenneth",
    "tony": "anthony",
    "dan": "daniel",
    "danny": "daniel",
    "dave": "david",
    "jim": "james",
    "jimmy": "james",
    "joe": "joseph",
    "zach": "zachary",
    "zack": "zachary",
    "ben": "benjamin",
    "gabe": "gabriel",
    "jon": "jonathan",
}

# D.J. -> DJ, Ja'Marr -> JaMarr, Amon-Ra -> AmonRa (curly apostrophe too)
_DROP_CHARS_RE = re.compile(r"[.'’\-]")  # noqa: RUF001
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]")


def fold_accents(raw: str) -> str:
    """`Andres Pena` <- `Andrés Peña`: NFKD, then drop the combining marks.

    MUST run before ``_NON_ALNUM_RE``/``[^a-z0-9 ]``, which would otherwise turn every
    non-ASCII letter into a space (``andr s pe a``) — making an accented spelling
    unmatchable against the ASCII spelling another source uses, and collapsing two
    different accented names onto one key.
    """
    return "".join(c for c in unicodedata.normalize("NFKD", raw) if not unicodedata.combining(c))


def _merge_initials(tokens: list[str]) -> list[str]:
    """['d', 'j', 'moore'] -> ['dj', 'moore'] so 'D J Moore' == 'D.J. Moore' == 'DJ Moore'."""
    out: list[str] = []
    buf = ""
    for tok in tokens:
        if len(tok) == 1:
            buf += tok
        else:
            if buf:
                out.append(buf)
                buf = ""
            out.append(tok)
    if buf:
        out.append(buf)
    return out


def normalize_name(raw: str) -> str:
    """Lowercase; drop periods/apostrophes/hyphens; collapse whitespace; merge initials;
    strip trailing suffixes (Jr, Sr, II, III, IV, V); alias the first token."""
    s = _DROP_CHARS_RE.sub("", fold_accents(raw).lower().strip())
    s = _NON_ALNUM_RE.sub(" ", s)
    tokens = _merge_initials(s.split())
    while len(tokens) > 1 and tokens[-1] in SUFFIXES:
        tokens.pop()
    if tokens:
        tokens[0] = ALIASES.get(tokens[0], tokens[0])
    return " ".join(tokens)


# ---------------------------------------------------------------------------
# Teams and DST
# ---------------------------------------------------------------------------

# (nflverse abbr, city, nickname, extra aliases). Aliases cover MFL/DynastyProcess
# (KCC TBB GBP NEP NOS SFO LVR LAR JAC SDC STL RAM OAK), PFR (KAN GNB NWE NOR TAM SFO
# SDG RAM OTI plus the five that look nothing like the team: CRD=ARI RAV=BAL HTX=HOU
# CLT=IND RAI=LV), ESPN (WSH LAR), Sleeper/Yahoo (LAR JAX) and common nicknames.
# `pfr` is one of the seven crosswalk sources, so every PFR spelling must be here.
TEAMS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("ARI", "Arizona", "Cardinals", ("ARZ", "CRD", "Cards")),
    ("ATL", "Atlanta", "Falcons", ()),
    ("BAL", "Baltimore", "Ravens", ("BLT", "RAV")),
    ("BUF", "Buffalo", "Bills", ()),
    ("CAR", "Carolina", "Panthers", ()),
    ("CHI", "Chicago", "Bears", ()),
    ("CIN", "Cincinnati", "Bengals", ()),
    ("CLE", "Cleveland", "Browns", ("CLV",)),
    ("DAL", "Dallas", "Cowboys", ()),
    ("DEN", "Denver", "Broncos", ()),
    ("DET", "Detroit", "Lions", ()),
    ("GB", "Green Bay", "Packers", ("GNB", "GBP")),
    ("HOU", "Houston", "Texans", ("HST", "HTX")),
    ("IND", "Indianapolis", "Colts", ("CLT",)),
    ("JAX", "Jacksonville", "Jaguars", ("JAC", "Jags")),
    ("KC", "Kansas City", "Chiefs", ("KAN", "KCC")),
    ("LA", "Los Angeles", "Rams", ("LAR", "RAM", "STL", "St. Louis Rams", "LA Rams")),
    (
        "LAC",
        "Los Angeles",
        "Chargers",
        ("SD", "SDG", "SDC", "San Diego Chargers", "LA Chargers"),
    ),
    ("LV", "Las Vegas", "Raiders", ("LVR", "RAI", "OAK", "Oakland Raiders")),
    ("MIA", "Miami", "Dolphins", ()),
    ("MIN", "Minnesota", "Vikings", ()),
    ("NE", "New England", "Patriots", ("NWE", "NEP", "Pats")),
    ("NO", "New Orleans", "Saints", ("NOR", "NOS")),
    ("NYG", "New York", "Giants", ("NY Giants",)),
    ("NYJ", "New York", "Jets", ("NY Jets",)),
    ("PHI", "Philadelphia", "Eagles", ()),
    ("PIT", "Pittsburgh", "Steelers", ()),
    ("SF", "San Francisco", "49ers", ("SFO", "Niners", "San Francisco Forty Niners")),
    ("SEA", "Seattle", "Seahawks", ()),
    ("TB", "Tampa Bay", "Buccaneers", ("TAM", "TBB", "Bucs")),
    ("TEN", "Tennessee", "Titans", ("OTI",)),
    ("WAS", "Washington", "Commanders", ("WSH", "Washington Football Team")),
)

# Tokens that mean "the defense" and carry no team information.
_DST_TOKENS: frozenset[str] = frozenset(
    {"dst", "def", "defense", "defence", "d", "st", "special", "teams", "team", "ds"}
)


def _clean(raw: str) -> str:
    s = fold_accents(raw).lower().replace("/", " ").replace("&", " ")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return " ".join(s.split())


def _build_team_lookup() -> dict[str, str]:
    lookup: dict[str, str] = {}
    cities = [city for _, city, _, _ in TEAMS]
    for abbr, city, nickname, aliases in TEAMS:
        keys = [abbr, nickname, f"{city} {nickname}", *aliases]
        if cities.count(city) == 1:  # "New York" / "Los Angeles" alone are ambiguous
            keys.append(city)
        for key in keys:
            k = _clean(key)
            assert k not in lookup or lookup[k] == abbr, f"team alias collision: {key!r}"
            lookup[k] = abbr
    return lookup


_TEAM_LOOKUP: dict[str, str] = _build_team_lookup()
_TEAM_BY_ABBR: dict[str, tuple[str, str]] = {abbr: (city, nick) for abbr, city, nick, _ in TEAMS}


def normalize_team(raw: str | None) -> str | None:
    """Any team spelling → nflverse abbreviation, or None (unknown, FA, ambiguous city)."""
    if not raw:
        return None
    s = _clean(raw)
    if not s:
        return None
    if s in _TEAM_LOOKUP:
        return _TEAM_LOOKUP[s]
    stripped = " ".join(t for t in s.split() if t not in _DST_TOKENS)
    return _TEAM_LOOKUP.get(stripped)


def normalize_dst(raw: str | None) -> str | None:
    """Any DST spelling → canonical ``"<abbr lowercase> dst"`` (e.g. ``"kc dst"``), or None."""
    abbr = normalize_team(raw)
    return f"{abbr.lower()} dst" if abbr else None


def canonical_dst_key(*candidates: str | None) -> str | None:
    """The DST key for the FIRST candidate that names a team — precedence is the argument
    order, and every writer must pass the same order.

    Both crosswalk writers used to inline their own fallback chain and they disagreed:
    ``apply_playerids`` tried team-then-name, ``resolve._canonical_name`` name-then-team.
    A row whose name and team point at *different* defenses ("Kansas City Chiefs" listed at
    "DEN") therefore canonicalized to a different player depending on which writer saw it
    first — a silently wrong mapping, which is exactly what the crosswalk exists to prevent.
    The agreed precedence is **name first** (DATABASE.md §3): the name is what the row is
    *about*, while the team column is a mutable weekly attribute.
    """
    for candidate in candidates:
        key = normalize_dst(candidate)
        if key is not None:
            return key
    return None


def dst_full_name(abbr: str) -> str:
    city, nickname = _TEAM_BY_ABBR[abbr]
    return f"{city} {nickname} DST"


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

FANTASY_POSITIONS: frozenset[str] = frozenset({"QB", "RB", "WR", "TE", "K", "DST"})

_POSITION_MAP: dict[str, str] = {
    "QB": "QB",
    "RB": "RB",
    "FB": "RB",
    "HB": "RB",
    "WR": "WR",
    "TE": "TE",
    "K": "K",
    "PK": "K",
    "DST": "DST",
    "DEF": "DST",
}


def normalize_position(raw: str | None) -> str | None:
    """'PK'→'K', 'FB'→'RB', 'DEF'/'D/ST'→'DST'; anything non-fantasy → None."""
    if not raw:
        return None
    key = re.sub(r"[^A-Z]", "", raw.upper())
    return _POSITION_MAP.get(key)
