# AI Interactions

`ffh.ai` is the **only** package permitted to call an LLM.

---

## 1. The rule that governs everything here

> **No LLM ever emits a projection, score, ranking value, probability, or any number that
> feeds a calculation.**

LLMs receive a pre-computed candidate set with all its numbers and they:

- rank and choose among the candidates
- argue for and against
- critique the other model's reasoning
- explain the recommendation in plain language
- extract structured facts from unstructured text (news, injury notes, trade messages)

They do not compute. An LLM-generated point projection loses to a Vegas-anchored regression
and — decisively — **cannot be backtested**. If you find yourself parsing a number out of a
model response and using it arithmetically, stop; that number belongs in `ffh.engine`.

The one permitted numeric output is a **confidence weight in [0,1]** attached to a ranking.
It is used to order and to flag, never as a multiplier on an engine value.

---

## 2. The debate protocol

```
  ENGINE  →  candidate set (6-8 options, each with full numbers)
     │
     │  build EVIDENCE PACKET (identical for both models)
     ▼
┌─────────────── ROUND 1 · INDEPENDENT ───────────────┐
│  Provider A  ──┐                      ┌── Provider B│
│  (randomized)  │   neither sees the   │             │
│                │   other's output     │             │
│  ranked rec    ┘                      └─ ranked rec │
└─────────────────────────────────────────────────────┘
     ▼
┌─────────────── ROUND 2 · REFUTATION ────────────────┐
│  Each model sees the other's argument, ANONYMIZED   │
│  ("Analyst B argued...").  Must explicitly REFUTE   │
│  specific claims or CONCEDE them.                   │
│  ★ This is the load-bearing round. ★                │
└─────────────────────────────────────────────────────┘
     ▼
┌─────────────── ROUND 3 · ADJUDICATION ──────────────┐
│  Judge (provider ALTERNATES, blind to authorship)   │
│  → final ranking, consensus score, disagreement axis│
└─────────────────────────────────────────────────────┘
```

### Why round 2 exists

Rounds 1 and 3 alone produce two confident parallel monologues and a judge picking on
style. **Forced refutation is what surfaces the actual disagreement** — it makes each model
engage with the specific claim it doesn't believe, rather than restating its own case.

### Consensus score is a product feature, not telemetry

```
consensus_score ∈ [0,1]
  = agreement on the top choice, weighted by how much each model conceded in round 2
```

| Score | UI behavior |
|---|---|
| ≥ 0.8 | Green. One-line rationale. Move on. |
| 0.5 – 0.8 | Neutral. Show the primary tension in one sentence. |
| < 0.5 | **Flagged.** Both cases shown side by side. This is a genuinely close call — worth thirty seconds of Chris's own attention. |

Surfacing disagreement instead of laundering it into false confidence is the honest design
and the differentiated one.

---

## 3. Bias controls — mandatory, not optional

Without these the "adversarial" framing is theater.

| Control | Implementation |
|---|---|
| **Randomize provider order** | Which provider is A vs B is drawn per debate. LLM judges have measurable position bias. |
| **Anonymize in round 2** | "Analyst A / Analyst B". Never reveal the model or vendor. |
| **Blind the judge** | The judge sees anonymized arguments only. |
| **Alternate the judge provider** | Round-robin per debate. Neither vendor gets home-field advantage. Log `judge_provider`. |
| **Identical evidence packets** | Byte-identical. Serialize once, send the same string to both. Assert this in a test. |
| **Freeze the JSON schema** | The schema is part of the prompt-cache key on both providers. Changing it silently invalidates the cache. Version schemas explicitly. |

---

## 4. Provider configuration

```python
DEFAULT = {
    "anthropic": "claude-sonnet-5",     #  $2 / $10 per Mtok
    "openai":    "gpt-5.6-terra",       #  $2 / $12 per Mtok
}
ESCALATE = {                            # high-stakes: early draft picks, big trades,
    "anthropic": "claude-opus-5",       # or when round-1 consensus < 0.5
    "openai":    "gpt-5.6-sol",
}
```

**Escalation triggers:** draft rounds 1–3; any trade above a value threshold; any debate
where round-1 consensus is below 0.5. Everything else runs on the default tier.

### Structured outputs — use them, they are hard constraints

- **Anthropic:** `output_config.format` with `type: "json_schema"`, and `strict: true` on
  tool definitions. Implemented as constrained sampling against a compiled grammar, so
  output is *guaranteed* schema-valid. Handle exactly three failure modes:
  `stop_reason: "refusal"`, `stop_reason: "max_tokens"` (truncated JSON is unparseable —
  **set `max_tokens` generously**), and enum casing drift.
  Unsupported keywords (`minimum`, `maxLength`, …) are stripped by the SDK and folded into
  descriptions; don't rely on them for validation.
- **OpenAI:** `response_format: {type: "json_schema", strict: true}`. Requires
  `additionalProperties: false`; model optionality via `anyOf` with null. **Check the
  `refusal` property and `finish_reason` before parsing.**

Parse failures should be ~0. Budget error handling for refusal and truncation only.

### Cost controls

At realistic volume this runs **~$25/mo across both providers**, or ~$17 with batching and
caching. It is not a cost problem — but three rules keep it that way:

1. **Batch API (50% off)** for anything on a weekly cadence — lineup, waiver, trade
   analysis. 24h turnaround is irrelevant for a Tuesday job.
2. **Prompt caching only where calls cluster.** ⚠️ **On a weekly cadence every call is a
   cache miss, and miss-plus-write costs ~11% MORE than not caching.** Enable caching
   per-workflow, not globally. It pays on draft day (calls seconds apart) and in batched
   weekly runs, nowhere else.
3. **Log `cost_usd`, `tokens_in`, `tokens_out`, `cache_hit` on every debate** into
   `ai_debates`. Budget drift should be visible, not discovered.

Anthropic bonus: cached input tokens don't count against ITPM, so a high cache-hit rate
also raises the effective throughput ceiling on draft day.

---

## 5. The evidence packet

Both models receive exactly this, byte-identical. **Everything numeric in it comes from the
engine.**

```jsonc
{
  "module": "draft",
  "league": {
    "num_teams": 12, "scoring": "half_ppr", "superflex": false,
    "roster": {"QB":1,"RB":2,"WR":3,"TE":1,"FLEX":1,"K":1,"DST":1,"BN":6}
  },
  "situation": {
    "pick_no": 14, "round": 2, "my_next_pick_no": 35,
    "picks_until_my_turn": 21
  },
  "my_roster": [
    {"player": "Ja'Marr Chase", "pos": "WR", "bye": 10}
  ],
  "roster_needs": {"RB": "critical", "TE": "moderate", "QB": "low"},
  "candidates": [
    {
      "player": "Saquon Barkley", "pos": "RB", "team": "PHI", "bye": 7,
      "projection_mean": 274.6, "gamma_shape": 8.1, "gamma_scale": 33.9,
      "vorp": 141.2, "vona": 68.4, "tier": 2, "tier_players_remaining": 3,
      "adp": 11.4, "adp_stdev": 4.2,
      "opponent_strength_next4": "favorable",
      "injury": {"status": null, "practice": "Full"},
      "notes": ["Beat reporter 2026-08-14: full participation in camp"]
    }
    // ... 5-7 more
  ],
  "engine_recommendation": {"top": "Saquon Barkley", "margin_over_2nd": 4.1},
  "context_news": [
    {"headline": "...", "source": "ESPN", "published": "2026-08-14T18:22:00Z"}
  ]
}
```

**`tier_players_remaining` vs `picks_until_my_turn` is the tier-cliff signal** — models
should reason about it explicitly, and prompts say so.

---

## 6. Response schemas

### Round 1 — independent recommendation

```jsonc
{
  "ranked": [
    {
      "player": "Saquon Barkley",
      "rank": 1,
      "confidence": 0.72,                    // [0,1] — ordering/flagging ONLY
      "primary_reason": "one sentence",
      "supporting_factors": ["...", "..."],
      "risks": ["..."]
    }
  ],
  "key_tension": "The single most important trade-off in this decision",
  "engine_disagreement": {
    "disagrees": false,
    "explanation": null                      // required non-null when disagrees is true
  }
}
```

`engine_disagreement` is deliberate: a model that thinks the engine is wrong must say so
explicitly and give a reason. Those cases are logged and reviewed — they're either a real
modeling gap or a hallucination, and both are worth knowing about.

### Round 2 — refutation

```jsonc
{
  "refutations": [
    {
      "target_claim": "quote the specific claim being challenged",
      "verdict": "refute",                   // refute | concede | partially_concede
      "argument": "why",
      "changes_my_ranking": true
    }
  ],
  "revised_ranking": ["Saquon Barkley", "..."],
  "position_changed": true
}
```

### Round 3 — judge verdict

```jsonc
{
  "final_ranking": [
    {"player": "...", "weighted_score": 0.0, "rationale": "..."}
  ],
  "consensus_score": 0.62,
  "disagreement_axis": "What they actually differed on — the real crux",
  "unresolved": ["Questions neither analyst settled"],
  "recommendation_summary": "2-3 sentences for the UI card"
}
```

⚠️ `weighted_score` is a **presentation ordering value derived from the judge's ranking and
the engine's own scores** — it is not a model-invented quantity fed back into any
calculation. Compute it in Python from the judge's ordinal ranking and engine values; do
not ask the model to produce it numerically.

---

## 7. Latency and degradation

**The engine result renders before the debate returns. Always.** The draft pick clock is
90 seconds.

```
t=0.0s   pick detected via poll
t<2.0s   engine recommendation ON SCREEN — complete, usable, correct
t~2.0s   debate kicked off async, card shows a "analyzing…" affordance
t<25s    debate streams into the already-visible card
```

| Failure | Behavior |
|---|---|
| One provider down/timeout | Single-model mode. Show it. `consensus_score = null`, no fake consensus. |
| Both providers down | Debate panel reads "unavailable". Engine recommendation stands unchanged. |
| Debate exceeds budget | Cancel. Log the timeout. Never block. |
| Schema refusal | Retry once with a fresh request; then degrade. Do not loop. |

**Nothing in the LLM path may ever gate a recommendation.** If a change makes the debate
load-bearing for correctness, that change is wrong.

---

## 8. Where the LLM layer is genuinely most valuable

Ranked by how much it beats the engine alone:

1. **Trade framing** — whether a rival manager will *accept* depends on how it's pitched
   and what he believes about his own roster. Not computable.
2. **News interpretation** — turning "limited in practice, expected to play" into a
   structured `p_active` prior the engine can consume.
3. **Close draft calls** — when VONA separates candidates by <5 points, qualitative
   factors (coaching change, camp reports, handcuff situation) legitimately break the tie.
4. **Explanation** — "Option B costs 0.4 expected points and gains 2.8% win probability
   because you're a 6-point underdog" is the output that makes the tool trustworthy.

Weakest uses, and why: anything numeric (the engine is better and testable), and anything
where the engine already has high confidence and a wide margin — burning tokens to confirm
an obvious call is waste. **Skip the debate entirely when the engine's top candidate leads
by more than a configured margin.** Log the skip.

---

## 9. Proving the layer earns its keep

Every debate is logged with the pre-debate engine output, the post-debate output, and the
eventual outcome (`recommendations` and `ai_debates` in [`DATABASE.md`](DATABASE.md) §7).

At season end — and ideally at a Week 8 checkpoint — answer:

- Did following the post-debate recommendation beat following the raw engine ranking?
- Did high-consensus debates outperform low-consensus ones? (They should.)
- Did either provider systematically outperform? (Watch for a judge-provider effect.)
- What did the whole layer cost, against that measured benefit?

**If the answer is that it added nothing, cut it.** Build the harness that can tell you.
It's roughly 30 lines and it's the difference between engineering and vibes.
