# Pipeline Triage Agent

An LLM-assisted triage tool for Airflow pipeline failures. It reads a failed
task's logs, diagnoses the root cause, explains it in two registers — one for
engineers, one for analysts — and opens a pull request with a suggested fix.
Nothing merges without human approval.

Ships with a local harness that generates realistic pipeline failures, so you
can run the whole thing on your laptop without a cloud account.

---

## Why

A DAG fails overnight. The on-call engineer opens the alert, finds the DAG,
scrolls the logs for the line that matters, cross-references the source in
GitHub, traces upstream tables, writes a fix, opens a PR, and explains to an
analyst why their report is stale. Most of that is mechanical, and most failures
fall into the same handful of shapes.

This compresses "wake up, investigate, diagnose, explain" into "wake up, review,
approve." The judgment stays with the human — the clicking doesn't.

---

## What it does

- **Lists failures** across environments, with filters by environment, date
  range, category, and repository
- **Diagnoses root cause** from the task log via the Anthropic API
- **Explains twice** — an engineer version and an analyst version, toggled in
  the UI, because whoever is on support isn't always someone who reads
  tracebacks
- **Rates its own confidence** (High / Medium / Low) alongside every suggestion
- **Suggests a fix** as a diff against the repository source
- **Opens a pull request** on approval — never merges

### Failure categories it handles

| Category | Example |
|---|---|
| `schema` | Upstream adds a column; the load rejects the batch |
| `merge` | Upsert fails on duplicate business keys in the source |
| `dq` | A data quality threshold is crossed and the pipeline halts |
| `sensor` | An upstream file never lands; the sensor times out |
| `table` | A table was renamed or dropped, or a grant is missing |
| `gcs` | A file doesn't arrive, or an export writes to a bad path |
| `auth` | Token-per-record inefficiency — slow rather than broken |

---

## Quick start

Requires Docker and an Anthropic API key.

```bash
git clone https://github.com/<you>/pipeline-triage-agent
cd pipeline-triage-agent

cp .env.example .env          # add ANTHROPIC_API_KEY
docker compose up -d          # Airflow + Postgres warehouse

python scripts/seed_warehouse.py     # deterministic fake data
```

Unpause the DAGs in the Airflow UI at `localhost:8080`, or trigger failures on
demand:

```bash
python scripts/scenarios/inject_duplicate_product_keys.py
python scripts/scenarios/add_column_to_orders_extract.py
python scripts/scenarios/reset.py     # restore the clean seed
```

Then start the agent and open the dashboard:

```bash
pip install -r requirements.txt
uvicorn agent.main:app --port 8787        # dashboard: http://localhost:8787
python -m agent.main                      # or print today's failures in the terminal
```

The dashboard lists every failed task in the chosen date range with its real
exception. Click a row for the task log, the diagnosis (engineer or analyst
view), and the suggested diff. **Raise PR** asks for confirmation and is only
enabled for High-confidence fixes when `GITHUB_REPO` is set. Without
`ANTHROPIC_API_KEY` you still get the failures and raw errors, just no diagnosis.

Open the dashboard through the agent rather than from disk: the agent injects
the API token when it serves the page, so the HTML file itself holds no secret.

---

## The local harness

The point of the harness is that **every failure is real**. The schema DAG fails
with a genuine driver error, the sensor DAG genuinely times out, the merge DAG
hits a genuine cardinality violation. Synthetic `raise Exception("schema error")`
tracebacks would prove nothing about whether the diagnosis works.

DAGs are generated from config rather than hand-written — eight templates in
`dags/dag_factory.py`, and as many DAGs as you list in
`dags/config/pipelines.yaml`. Add entries to scale up.

---

## Architecture

```
┌──────────────┐        JSON         ┌──────────────┐
│  Dashboard   │ ◄─────────────────► │   Agent      │
│  (browser)   │                     │  (FastAPI)   │
└──────────────┘                     └──────┬───────┘
                                            │
                    ┌───────────────────────┼───────────────────┐
                    ▼                       ▼                   ▼
              Airflow REST API        Postgres            Anthropic API
              (task logs)             (warehouse)         (diagnosis + fix)
                                            │
                                            ▼
                                       git / gh
                                    (branch + PR)
```

The agent returns one object per failure. The two things worth noting in the
shape: `cause` carries both registers as sibling fields, and `confidence` sits
next to the suggested diff rather than being inferred after the fact.

<!-- TODO: paste the trimmed JSON contract here once the shape is settled -->

---

## Security

This tool can write to a repository and run CLI commands, so it's built with the
posture you'd give any service with production write access:

- **Token header required on every endpoint.** Without it, any local process —
  or a stray browser tab — could trigger real git operations.
- **Repository, branch, and file path validated against an allow-list**, closing
  off path traversal outside the intended checkout.
- **CORS locked to the app's own origin**, so the token can't be read by a page
  on another site.

An agent that writes code and an agent that operates infrastructure are not the
same risk category. This is the second kind.

---

## Limitations

- **Single-turn questions only.** You ask one question about a failure and get
  one answer; there's no conversation thread, so follow-ups lose context.
- **No caching.** Every poll re-runs diagnosis for every open failure, which is
  fine locally and wasteful at scale.
- **Local credentials.** The agent runs under developer CLI sessions rather than
  a dedicated service account. A production deployment belongs in a container
  with its own scoped credentials.
- **The model is confidently wrong about systems it can't see.** It reasons well
  inside the boundary of the code and logs available to it, and assumes poorly
  outside that boundary. This is why nothing merges without review.

---

## Roadmap

- [ ] Conversational thread per failure
- [ ] Diagnosis caching
- [ ] Containerized deployment with a scoped service account
- [ ] Additional failure categories

---

## License

MIT
