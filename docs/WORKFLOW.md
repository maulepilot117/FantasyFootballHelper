# Workflow

**Claude Code is the primary builder and architect. Codex writes code and performs
adversarial review.** Everything lands in GitHub; ArgoCD reconciles to the homelab.

---

## Division of labor

| | Claude Code | Codex |
|---|---|---|
| Architecture and design decisions | ✅ owns | proposes, challenges |
| Spec writing (`docs/*.md`) | ✅ owns | reviews for gaps |
| Implementation | ✅ | ✅ — on well-specified units |
| **Adversarial review** | reviews Codex's work | ✅ **owns — required gate** |
| Migrations | ✅ owns | reviews |
| Merge decision | ✅ | — |

Codex's job is to be the check, not the rubber stamp. A review that finds nothing should
say so plainly — manufactured findings are worse than none. See [`AGENTS.md`](../AGENTS.md)
for the review criteria.

---

## Branch and commit

```
main                     protected, always deployable
feat/<area>-<short>      feat/draft-vona
fix/<area>-<short>       fix/crosswalk-suffix-normalization
chore/<short>            chore/bump-nflreadpy
docs/<short>             docs/engine-faab-worked-example
```

Conventional commits, scoped to the module:

```
feat(engine): VONA via ADP distribution sampling
fix(crosswalk): normalize Jr./III suffixes before matching
test(engine): worked-example fixtures from ENGINE.md
docs(data): correct nflverse stats_player_week asset path
```

**Never commit to `main` directly.** Never force-push a shared branch.

### Root checkout stays on `main`; feature work lives in worktrees

The repo root (`FantasyFootballHelper/`) is the **read-only mirror of `main`**. Never check
out a feature branch there. Every unit of work gets its own worktree under
`.claude/worktrees/<branch>` (Claude Code's `EnterWorktree` does this automatically).

After a PR merges, close the loop **from the root**:

```
git switch main && git pull --ff-only
git worktree remove .claude/worktrees/<name>   # or let Claude Code clean it up on exit
git branch -d <branch>
```

If the root ever shows a feature branch or is behind `origin/main`, that is a bug in the
workflow — fix it with the commands above before starting new work. Symptom to watch for:
"the code isn't in the root, it's only in `.claude/worktrees/`" — that means the root was
never switched back to `main` after a merge.

---

## Definition of done

A PR is not done until **all** of these hold:

- [ ] Tests pass, including the engine-purity test and the crosswalk coverage tests
- [ ] `ruff check` and `ruff format --check` clean; `bun test` passes for frontend changes
- [ ] New engine math has a **unit test against a hand-computed fixture**
      ([`ENGINE.md`](ENGINE.md) carries expected values — use them)
- [ ] The relevant `docs/*.md` is updated **in the same PR**, not a follow-up
- [ ] Any schema change has an Alembic migration and a `DATABASE.md` update
- [ ] Codex adversarial review complete and every BLOCKING finding resolved
- [ ] No secret, key, cookie, or token anywhere in the diff

---

## The Codex review gate

Every PR gets a Codex pass before merge. Hand it the diff plus the relevant spec doc, and
ask for the review format in [`AGENTS.md`](../AGENTS.md).

```
VERDICT: BLOCK | APPROVE WITH NOTES | APPROVE

[BLOCKING] file.py:42 — one-sentence defect
  Failure: concrete inputs → wrong output
  Fix: specific change
```

**Resolving a BLOCKING finding means fixing it or writing down why it's wrong.** Silently
dismissing it defeats the gate. If Claude Code and Codex disagree on a design point,
escalate to Chris with both arguments stated — don't let the tie break on whoever wrote
last.

### What the reviewer should hunt for

This system's dominant failure mode is **silent wrongness**: a plausible-looking number
that is wrong. A passing test suite and a working UI will not catch it. Priority order:

1. **Crosswalk gaps** — fail as missing players, never as exceptions
2. **Silent row loss in Polars joins** — the default drops non-matching rows
3. **Hardcoded scoring settings** — a bug even when the default happens to be right
4. **Week/season boundary errors** — off-by-one, byes as zeros, the 18-week 2026 schedule
5. **Timezone bugs** — lock is Thu 8:15pm ET, nflverse is UTC, Sleeper `last_picked` is epoch ms
6. **Distribution collapsed to a mean** — breaks every win-probability calculation downstream

---

## Testing

| Layer | Standard |
|---|---|
| `engine/` | **Highest bar.** Pure functions, hand-computed fixtures, property tests where the math has invariants (VORP ordering, PSD correlation matrix, Gamma params round-trip) |
| `crosswalk/` | Coverage tests are **mandatory and blocking** — see [`DATABASE.md`](DATABASE.md) §3 |
| `adapters/` | Recorded fixtures (VCR-style). Never hit live APIs in CI. |
| `ingest/` | Idempotency: running twice produces one result, not two |
| `ai/` | Schema validation, bias controls (identical packets, judge alternation), and degradation paths. **Do not assert on model prose.** |
| `api/` | Contract tests against the shapes in [`API.md`](API.md) |

**Never call a live LLM or a live platform API in CI.** Debate tests use recorded responses.

---

## Adding a data source

Ordered, because skipping step 1 is how you build on a dead package:

1. **Verify it live.** [`DATA_SOURCES.md`](DATA_SOURCES.md) documents several sources whose
   current state contradicts pre-2026 training data. Check the URL responds and the shape
   is what you expect before writing code.
2. Add it to `DATA_SOURCES.md` — URL, auth, cadence, **license**, and gotchas.
3. Write the ingest job: idempotent, watermarked in `ingest_runs`, `If-None-Match` where
   supported, lands Parquet in a **new partition** (never overwrite a scrape).
4. Wire crosswalk resolution for any player identifiers it carries.
5. Add a CronJob in `deploy/` at the cadence the source actually updates.
6. If it's an undocumented endpoint: wrap with cached last-known-good and surface staleness.

---

## Working against the deadline

The 2026 season opens **Wed Sept 9**; peak draft weekend is **Sept 4–7**. Until the draft
module ships:

- **Cut scope, not correctness.** An ugly correct draft board beats a beautiful late one.
- **The static cheat sheet export is required, not optional.** It's the floor when
  everything else fails.
- **Mock drafts are the test harness.** Sleeper mocks are free and unlimited. Run several
  end-to-end before draft day — the live poller *will* have a bug and you don't want to
  find it at pick 1.03.
- Anything not on the draft critical path goes in [`ROADMAP.md`](ROADMAP.md) phase 2 and
  waits.

---

## Session start checklist

For Claude Code picking up work in a new session:

1. Read [`ROADMAP.md`](ROADMAP.md) — what phase are we in, what's in scope
2. Read the spec doc for the area being touched (see the table in `CLAUDE.md`)
3. `git log --oneline -20` and check open PRs for in-flight work
4. Confirm the scope with Chris before starting anything that spans multiple modules
