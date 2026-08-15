# Engine — Quantitative Specification

Everything in `ffh.engine` and `ffh.projections`. **This package is pure: no I/O, no
network, no LLM calls.** It takes features and league config in, returns scored candidates
out. A test enforces this — it is what makes the system backtestable.

Worked examples below carry expected values. **Use them as unit-test fixtures.**

---

## 1. Scoring

Scoring is always read from the platform, never assumed. Normalize to:

```python
class ScoringSettings(BaseModel):
    pass_yd: float; pass_td: float; pass_int: float; pass_2pt: float
    rush_yd: float; rush_td: float
    rec: float          # 1.0 PPR / 0.5 half / 0.0 standard
    rec_yd: float; rec_td: float
    te_rec_bonus: float = 0.0
    fumble_lost: float
    bonus_rules: list[BonusRule] = []   # e.g. 100+ rush yards → +3
    # ... K and DST rules
```

Every projection and every historical actual is scored **through this object**. A player's
value is league-specific; there is no such thing as "his projection."

---

## 2. Value Based Drafting

All three metrics share one form; only the baseline differs.

```
VBD(player) = projected_season_points(player) − baseline(position)
```

### VOLS — Value Over Last Starter

```
baseline_rank(pos) = n_teams × n_starters(pos)
```
FLEX-eligible positions get the flex slots distributed pro-rata by historical flex usage
(roughly RB 0.45 / WR 0.45 / TE 0.10 — tune from league history when available).

### VORP — Value Over Replacement Player

Baseline is the **best player at that position who is not rostered anywhere** — the actual
waiver-wire alternative.

```
baseline_rank(pos) = n_teams × (n_starters(pos) + expected_bench_held(pos))
```

`expected_bench_held` captures positional hoarding — leagues stash 2–3 RBs deep and almost
no TEs. Derive from league history; fall back to `{RB: 2.5, WR: 2.5, TE: 0.5, QB: 0.5,
K: 0, DST: 0.2}`.

**Worked example** (12-team, half-PPR):

| Player | Proj | Baseline | VORP |
|---|---|---|---|
| Ja'Marr Chase (WR) | 283.3 | 114.2 (WR ~60) | **169.1** |
| Lamar Jackson (QB) | 371.1 | 244.4 (QB ~18) | **126.7** |

Note the ordering flip vs. raw points: Lamar outscores Chase by 88 but is worth 42 *less*,
because replaceable QB production is abundant. That inversion is the entire point of VBD
and is the first thing to assert in tests.

### VONA — Value Over Next Available ⭐

**The differentiating metric.** VORP and VOLS are precomputable and therefore generic.
VONA answers the only question that matters at the table: *given the board right now and
who picks before me again, what do I lose by waiting?*

```
VONA(player) = projection(player) − E[best available at that position at my next pick]
```

It requires simulation and cannot be precomputed.

```python
def vona(player, board, my_next_pick_no, current_pick_no, adp_table, n_sims=5000):
    """For each sim, sample which players come off the board between now and my
    next pick, then record the best survivor at each position."""
    survivors_by_pos = defaultdict(list)
    for _ in range(n_sims):
        gone = set()
        for slot in range(current_pick_no + 1, my_next_pick_no):
            # Sample the pick from the ADP distribution, not the point ADP.
            pick = sample_pick(board, gone, slot, adp_table)   # see below
            gone.add(pick)
        for pos in POSITIONS:
            best = max((p for p in board if p.pos == pos and p.id not in gone),
                       key=lambda p: p.projection, default=None)
            survivors_by_pos[pos].append(best.projection if best else 0.0)
    return player.projection - mean(survivors_by_pos[player.pos])
```

**`sample_pick` must sample from the ADP distribution, not take the point ADP.**
For each undrafted player compute `z = (slot − adp) / adp_stdev`, weight by
`exp(−z²/2)`, normalize, and draw. A degenerate point-ADP simulation always produces the
same board and VONA collapses to a constant. `adp_stdev` is required for this reason
(see [`DATABASE.md`](DATABASE.md) §5).

**Worked example** — pick 1.02, next pick 2.11 (snake, 12-team), 20 picks in between:

| Player | Proj | E[best available at 2.11] | VONA |
|---|---|---|---|
| Justin Jefferson (WR) | 291.4 | 252.0 | **39.4** |
| Saquon Barkley (RB) | 274.6 | 206.2 | **68.4** |

Jefferson projects 17 points higher, but RB depth collapses far faster over the next 20
picks, so Barkley is the correct pick by 29 points of positional cost. **VONA outranks
VORP as the primary draft signal.** VORP is the tiebreaker and the sanity check.

### Performance

VONA runs on every candidate on every pick, inside a 2-second budget. Vectorize with
NumPy over sims rather than looping in Python, precompute the ADP weight matrix once per
draft, and **warm the cache during the 60–90s of dead time between picks** by running VONA
for the several most plausible board states.

---

## 3. Tiers

**Algorithm:** 1-D Gaussian Mixture Model over expert consensus rank (FantasyPros ECR via
DynastyProcess). Select `k` by **BIC minimum**.

```python
from sklearn.mixture import GaussianMixture
import numpy as np

def fit_tiers(ecr_ranks: np.ndarray, k_range=range(2, 13)):
    X = ecr_ranks.reshape(-1, 1)
    models = {k: GaussianMixture(k, random_state=0, n_init=5).fit(X) for k in k_range}
    best_k = min(models, key=lambda k: models[k].bic(X))
    return models[best_k].predict(X), best_k
```

Weight by ECR dispersion where available — a player with high `sd` across experts sits
less firmly in his tier and that uncertainty should widen the component.

**The output that matters is not the tier label — it's tier-cliff distance:**

```
tier_cliff(pos) = (# players remaining in the current tier at pos)
                  vs (# picks until my next selection)
```

When players-remaining ≤ picks-until-my-next-turn, **the tier will be gone**. That is the
signal that converts a tier chart into a decision, and it composes directly with VONA
(a cliff is precisely what makes E[best available] drop).

Surface it as: *"3 WRs left in tier 2, 9 picks until your next — this tier will not
survive."*

---

## 4. Projections

**Output a distribution, not a point estimate.** Win probability, the start/sit variance
rule, FAAB valuation, and playoff odds are all impossible without it. Retrofitting later
means rewriting every decision module.

### Step 1 — anchor on Vegas

```
implied_home_total = total_line / 2 + spread_line / 2
implied_away_total = total_line / 2 − spread_line / 2
```
(`spread_line` positive favors home, per nflverse convention.)

**Worked example:** `total_line = 47.5`, `spread_line = 3.5`
→ home 25.50, away 22.00. Sum = 47.5 ✓, difference = 3.5 ✓.

### Step 2 — game script

Trailing teams pass more. This is a large, real, well-documented effect and it is the main
reason a naive projection misses.

```
pass_rate = base_team_pass_rate + β · expected_trailing_margin
```
Fit `β` from play-by-play: regress in-game pass rate on score differential by quarter.
Expected trailing margin comes from the spread.

### Step 3 — distribute by usage

```
player_expected_targets  = team_pass_attempts × target_share
player_expected_carries  = team_rush_attempts × carry_share
player_expected_rz_touch = team_rz_plays × rz_share
```

`target_share`, `air_yards_share`, `wopr`, `racr` are **precomputed** in
`stats_player_week_{YEAR}.parquet` — do not rebuild them. Use a recency-weighted blend
(exponential decay, half-life ~4 games) rather than a season average, and **use snap % as
the route-participation proxy** since real routes-run is post-season only.

### Step 4 — context adjustments

Multiplicative, each centered on 1.0:

| Adjustment | Source | Notes |
|---|---|---|
| Opponent defense | EPA/play allowed by position from pbp | We compute this; DVOA is paywalled and not needed |
| Weather | Open-Meteo forecast + stadium heading | **Wind dominates.** Passing and kicking degrade sharply above ~15mph; use the crosswind component from `wind_dir_deg` vs `stadiums.heading_deg`. Skip entirely when `games.roof` is `dome` or `closed`. |
| Altitude | `stadiums.altitude_ft` | Denver only, small effect, mostly on kicking |
| Rest / travel | `home_rest`, `away_rest`, `neutral_site` | 2026 has a Melbourne game — handle it |
| Injury | `player_injury_status` | See below |

**Injury handling — two separate multipliers, don't conflate them:**
- `p_active` — probability he plays at all. Out → 0. Doubtful → ~0.25. Questionable → ~0.70.
  Calibrate from historical `report_status` → actual-played rates in nflverse.
- `effectiveness | active` — a Questionable-with-DNP-Wednesday player who plays is
  typically diminished. Practice participation (DNP/Limited/Full) is the signal here.

`E[points] = p_active × E[points | active] × effectiveness`. Note the variance also rises,
which the start/sit rule will correctly exploit.

### Step 5 — fit a Gamma

Fantasy scores are non-negative and right-skewed. Gamma is the correct family.

```
k = μ² / σ²          (shape)
θ = σ² / μ           (scale)
```

**Worked example:** μ = 14.0, σ = 7.0 → σ² = 49 → **k = 4.0, θ = 3.5**.
Verify: `kθ = 14.0` ✓, `kθ² = 49.0` ✓.

⚠️ **Calibrate σ from realized weekly score variance, not from cross-source projection
disagreement.** Projection disagreement measures how much experts argue about a player's
*season*; it is not his weekly boom/bust volatility. Using it here is a subtle and
consequential error.

Estimate σ from the player's own recent weekly variance, shrunk toward the
position-and-role mean (a low-volume TE and a bell-cow RB have very different variance
profiles). Use empirical Bayes shrinkage — small samples otherwise produce absurd σ.

---

## 5. Correlation

Independent player draws badly misstate lineup variance. Correlate with a Gaussian copula.

```python
# 1. Build correlation matrix Σ over the players in scope
# 2. Draw Z ~ MVN(0, Σ)
# 3. U = Φ(Z)                        → correlated uniforms
# 4. X_i = Gamma_i.ppf(U[:, i])      → correlated scores with the right marginals
```

Starting correlations (refine from pbp):

| Pair | ρ |
|---|---|
| QB ↔ own WR1 | +0.45 |
| QB ↔ own WR2/TE | +0.30 |
| QB ↔ own RB | +0.05 |
| Same-team WR ↔ WR | −0.10 (they compete for targets) |
| Opposing-team skill players (shootout) | +0.15 |
| DST ↔ opposing offense | **−0.55** |
| Kicker ↔ own offense | +0.25 |

Stacking and negative-hedging then **fall out of the model** rather than being bolted on
as rules. Ensure Σ is positive semi-definite before use — clip negative eigenvalues.

---

## 6. Start/sit — maximize P(win), not E[points] ⭐

**The most under-implemented idea in consumer fantasy tools.**

Let `μ` = expected margin (your lineup minus opponent's) and `σ` = std dev of that margin.
Approximating the margin as normal:

```
P(win) = Φ(μ / σ)
```

| Situation | Consequence |
|---|---|
| **μ < 0 — you're the underdog** | `P(win)` is **increasing** in σ → **maximize variance.** Start the boom/bust player. |
| **μ > 0 — you're favored** | `P(win)` is **decreasing** in σ → **minimize variance.** Start the safe floor. |
| μ ≈ 0 | σ is roughly neutral — decide on expectation |

**Worked example.** Projected 112.0 vs opponent 118.0, so μ = −6.0.

| Option | σ (margin) | μ/σ | P(win) |
|---|---|---|---|
| A — safe floor WR | 22.0 | −0.2727 | **0.393** |
| B — boom/bust WR | 30.0 | −0.2000 | **0.421** |

**Start B.** It has the same expected points and a 2.8-point-higher win probability
purely from variance. Flip the sign — μ = +6.0 — and A wins 0.607 to 0.579.

For the real recommendation, don't use the closed form: simulate the full lineup against
the opponent's projected lineup with the copula, count the fraction of sims you win. The
closed form is the intuition and the unit test.

**Always show the trade-off explicitly in the UI.** "Option B costs 0.4 expected points
and gains 2.8% win probability because you're a 6-point underdog" is a far better
explanation than a bare ranking, and it's the kind of reasoning the LLM layer should
elaborate on.

---

## 7. Lineup optimization

MILP via **PuLP** (pure Python, bundles the CBC solver — no ARM build problem).

```
maximize   Σ_i  value_i · x_i
subject to Σ_i x_i[slot]  = required(slot)   for each roster slot
           Σ_slots x_i    ≤ 1                each player fills at most one slot
           x_i ∈ {0,1}
```

`value_i` depends on the situation:
- Clear favorite or clear underdog → optimize the **win-probability-adjusted** value from §6
- Otherwise → expected points

Because the win-prob objective is not linear in the player set, run it as: enumerate the
top-N candidate lineups by expected points (N ≈ 50), simulate each, pick the highest
P(win). N=50 simulations at 10k draws each is well inside the offline batch budget.

---

## 8. Waiver

### Value of an add

**ΔVORP against *your specific roster*, not a generic ranking.**

```
ΔVORP(add, drop) = VORP_contribution(roster + add − drop)
                   − VORP_contribution(roster)
```

Where `VORP_contribution` accounts for the fact that a 4th good RB on your roster is worth
much less than your first — measure the change in *expected starting lineup points across
the remaining weeks*, including bye coverage and injury replacement value.

Also compute `Δ P(playoffs)` from the season simulation (§10). Late in the season that is
the objective, and it can diverge sharply from ΔVORP.

### FAAB bidding ⭐

Structurally this is a **sequential first-price sealed-bid auction under a hard seasonal
budget constraint**. Two consequences most tools ignore:

**(a) Allocate budget by marginal value share.**

```
bid_base = remaining_budget × ΔVORP(this) / (ΔVORP(this) + E[Σ future ΔVORP])
```

The denominator shrinks as the season progresses, which correctly makes you more
aggressive in Week 12 than Week 2. Estimate `E[Σ future ΔVORP]` from the historical
distribution of waiver-add values by week.

**(b) Shade for first-price competition.**

Standard symmetric first-price equilibrium with `n` bidders: bid `((n−1)/n) × value`.
Note the direction — **more competition means bidding *closer* to value, not less.**

```
bid = bid_base × (n − 1) / n
```

Estimate `n` from Sleeper trending adds, ownership %, and how many league rosters actually
have a hole at that position (we know every roster, so compute it — don't guess).

**Worked example:** remaining budget $73, ΔVORP 18.0, E[Σ future ΔVORP] 62.0, n = 4
expected bidders.

```
bid_base = 73 × 18 / (18 + 62) = 73 × 0.225 = $16.43
bid      = 16.43 × 3/4         = $12.32  →  bid $12
```

Report the full curve, not just the number: *"$12 wins ~55% of the time, $19 wins ~80%."*
That's the actual decision.

---

## 9. Trade evaluation

**Keep two value curves strictly separate. The gap between them is the product.**

| Curve | Source |
|---|---|
| **Model value** | Our own VORP surplus, rest-of-season, league-scoring-specific |
| **Market value** | FantasyCalc (~1M real trades). Consensus, not truth. |

```
arbitrage(player) = percentile(model_value) − percentile(market_value)
```

- Strongly positive → **buy low.** The market undervalues him relative to our projection.
- Strongly negative → **sell high.**

Percentile-normalize before differencing; the two curves are on different scales and
raw subtraction is meaningless.

### Rival roster analysis — computed, not guessed

We have every roster. Compute, don't speculate:

- Positional holes: starting-lineup weakness by slot vs. league median
- Bye-week collisions in the coming weeks
- Injury exposure at each position
- Depth surplus — a manager with 5 startable WRs and 2 RBs is a real trade partner

### Objective

Rank packages by **Δ P(playoffs) for us** (from §10), not by raw value delta. A trade that
wins on points but wrecks a bye week can lower our playoff odds.

Each proposal reports: our Δ value, their Δ value (so it's plausible they accept), Δ
P(playoffs) for us, and the specific need on their roster it addresses.

**This is where the LLM layer earns the most.** Whether a manager will *actually accept*
a trade depends on how it's framed and what he believes about his own team — that's not
computable, and it's exactly the fuzzy judgment the debate layer handles well.

---

## 10. Season Monte Carlo

```
for sim in 1..N:                     # N = 10_000 (interactive) to 100_000 (offline)
    for week in remaining_weeks:
        draw each team's score  = Σ correlated player draws for their projected lineup
        resolve the ACTUAL head-to-head schedule
    accumulate W/L + points-for
    apply the league's real playoff and tiebreaker rules
report P(playoffs), P(bye), P(title), full seed distribution
```

⚠️ **Chunk the simulation.** A vectorized `(n_sims × n_players)` array is the realistic OOM
cause on 8–16GB Pi nodes. Process in batches of ~1,000 sims and accumulate.

### Schedule luck decomposition

Cheap to build, high perceived value, and it corrects a real misjudgment about your own
team.

```
all_play_record = for each week, your score vs EVERY other team's score that week
schedule_luck   = actual_win_pct − all_play_win_pct
```

A team at 4-6 with a .620 all-play record is genuinely good and unlucky; treat it as a
buy-low signal in the trade module, not as a team that needs blowing up.

---

## Test fixtures — expected values

| Test | Expected |
|---|---|
| VORP ordering inversion | Lamar (371.1) VORP 126.7 < Chase (283.3) VORP 169.1 |
| Implied totals | total 47.5, spread 3.5 → home 25.50, away 22.00 |
| Gamma params | μ=14.0, σ=7.0 → k=4.0, θ=3.5 |
| Underdog variance rule | μ=−6, σ 22→30 raises P(win) 0.393→0.421 |
| Favorite variance rule | μ=+6, σ 22→30 lowers P(win) 0.607→0.579 |
| FAAB | budget 73, ΔVORP 18, future 62, n=4 → base $16.43, shaded $12.32 |
| VONA beats VORP on positional cliff | Barkley VONA 68.4 > Jefferson VONA 39.4 despite lower projection |
| Correlation matrix | Σ is positive semi-definite after eigenvalue clipping |
