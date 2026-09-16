# AGENTS.md

Standing instructions for any coding agent working in this repo (Claude Code,
Antigravity, Codex, Cursor). Read this before doing anything.

## What this repo is

A payment settlement lakehouse: Kafka, SeaweedFS (S3), PySpark, dbt-duckdb,
ClickHouse, orchestrated by Airflow 3.3.1. It must fit in a 6 GB Docker VM.
The memory budget is a design constraint, not a bug: read
`docs/adr/0003-profiles-and-the-6gb-budget.md` before touching any `MEM_*`.

## Definition of done

Work is done when **both** pass, and not before:

1. `make lint && make test` (no Docker; CI workflow `tests`)
2. `make verify` (full stack; CI workflow `e2e`)

`make verify` exits 0 only if the DAG run succeeds, no `np_*` container
restarted or was OOM-killed, no init container failed, and
`mart_settlement_reconciliation` has rows. On failure, read
`verify-report/summary.md` first, then the files it points to. Do not ask
the human to paste logs: the evidence is in `verify-report/` locally, or in
the `verify-report` artifact of the `e2e` run on GitHub.

## The loop

1. Work on a branch named `agent/<short-topic>`. Never commit to `main`.
2. Form one hypothesis from the evidence. Write it in the commit message.
3. Make the smallest change that tests it. Commit.
4. Run `make verify` (or push and let `e2e` run it). Read the report.
5. Repeat until green. Then open a PR whose description lists: the root
   cause, the evidence that confirmed it, what changed, and the final
   `summary.md`.

Stop and ask the human only if:

- the fix needs the total memory budget raised above ~5.5 GB, or
- the fix changes a public contract (mart schemas, topic names, bucket layout), or
- five iterations have not moved the failing check.

Anything else that is reversible: choose the conservative option, note the
assumption in the PR, and keep going.

## Rules

- **Sole user attribution on all commits and PRs.** Only Laurence's credentials (`laurence702 <akaigbokwelaurence@gmail.com>`) must appear as author and committer. Never include `Co-Authored-By`, `Claude-Session`, AI signatures, or AI trailers in commit messages, PR descriptions, or git tags.
- **Production-ready code comments and docstrings.** All comments, docstrings, and documentation must be clean, senior-engineer production quality. No comments suggesting work-in-progress, debugging relics, temporary hacks, or conversational/AI self-narration.
- **Commit at natural breakpoints at regular intervals.** Structure changes into logical, human-like incremental commits (e.g. schemas/models -> core logic -> tests -> configuration/docs) with concise, professional messages explaining *why*. Never dump an entire finished project or multi-feature codebase in a single massive commit.
- If you raise one `MEM_*`, lower another, and update the budget table in
  `.env.example` and ADR 0003 in the same commit.
- Pin versions. No `latest` tags.
- Money is integer kobo end to end. Never floats.
- Every change to a `src/` module comes with a test in `tests/`.
- Do not edit `HANDOVER.md` as a status report. It is a narrative writeup.

## Open issue (as of 2026-09-16)

SeaweedFS (`np_seaweedfs`) is OOM-killed during Spark writes. The kernel log
from `scripts/capture_seaweedfs_restart.sh` shows `weed` at ~860 MB RSS
against `MEM_S3=768m`, which `.env.example` itself marks as an unmeasured
estimate. Go's GC grows toward whatever memory is available unless told
otherwise. Hypotheses to test, in order:

1. Set `GOMEMLIMIT` on the seaweedfs service a little under `MEM_S3`, so the
   GC works to stay inside the cgroup instead of being killed at it.
2. If that is not enough, measure the peak from `verify-report/mem-samples.txt`
   and rebalance `MEM_S3` against another service within the budget.
