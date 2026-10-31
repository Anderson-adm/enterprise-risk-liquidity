# AGENTS.md — shared instructions for agents and humans

## Goal and boundaries

Seven synthetic sources → Bronze → dbt Silver/Gold → quality gate → transactional SQL Server
publication, orchestrated by Airflow. Keep the portfolio's macro architecture and business rules.
Current record: [docs/HANDOFF.md](docs/HANDOFF.md). Ownership and migration map:
[docs/REFACTORING.md](docs/REFACTORING.md). Layout follows data-project-engineering v1.3.

| Owner | Implementation |
|---|---|
| `apps/hybrid-runner/` | existing `erl` CLI, configuration/checkpoints, SQL extraction, local composition, AWS service calls; own extraction SQL |
| `apps/publisher/` | package validation/export, transactional publisher, own consumer SQL |
| `apps/source-generator/` | synthetic fixtures and scenarios; caller supplies connections through `SourceEnvironment` |
| `lambda/{market-data,internal-xml,external-xml}/` | own handler and parser, dependency and contract declaration in `unit.json` |
| `glue/{db-gb,db-tb,gold-load}/` | own entry point, native Spark code and pinned SDK requirements |
| `contracts/` | published Bronze/consumer/XSD/source-schema interfaces; one source of truth |
| `protocols/integrity-v2/erl_protocol/` | explicit `2.0.0` integrity dependency, vendored per artifact; hash/commit/checksum/atomic I/O guarantees |
| `dbt/`, `airflow/`, `infra/` | model ownership, orchestration, project/environment Terraform state |

## Permanent engineering rules

* Own implementation belongs to its executable unit. No global implementation root, private imports
  between units, empty scaffolds or default shared library. Public integrations go in `unit.json`.
  The runner deliberately consumes publisher/generator and parser APIs for local composition.
* The pinned integrity dependency is the exception for guarantees that must not diverge. Its version
  describes public contract compatibility, not implementation bytes: compatible internal edits do not
  automatically require a bump. Test affected consumers; artifact SHA-256 identifies the built bytes.
  Vendor only required modules into each artifact.
  Format-specific parsing and Spark operations remain local. No mutable global runtime ZIP.
* Contracts precede implementation. Preserve hash v2/legacy separation, commit visibility,
  immutable deliveries, quarantine, exact decimals, consumer-only ASCII and compatible checkpoints.
* Python 3.12, shared code compatible with Glue Python 3.11. Native PySpark expressions; no UDFs or
  unbounded driver materialisation. Close connections and multipart uploads even on failure.
* SQL is versioned beside its consumer; values are binds and identifiers use allowlists. Source
  schema/view DDL in `contracts/source_schemas.py` defines synthetic fixtures, not pipeline queries.
* `Timestamp`/`Dec` in the integrity dependency preserve DATETIME2 and exact SQLite decimals.
  SQL execution, publication and business rules are unchanged by an ownership move.
* Build before Terraform. Per-unit artifacts and hashes live in ignored `dist/`. Independent changes
  must preserve unrelated artifact bytes; a declared consumer may legitimately rebuild.
  Glue uses immutable content keys for script/library coherence; deploy with jobs idle.
* DB tests use `db_settings`: SQLite by default; real engines only with `ERL_IT_CONTAINERS=1` and
  `-m integration`. Keep local data/evidence isolated. Label mocks/import tests honestly.
* Small coherent commits ending in an agent attribution line. On this Windows drive
  (`core.filemode=false`), mark new shell scripts executable with `git update-index --chmod=+x`.

## Checks

```bash
uv sync --frozen
uv run ruff check apps protocols contracts tests airflow glue lambda scripts
uv run ruff format --check apps protocols contracts tests airflow glue lambda scripts
DBT_SEND_ANONYMOUS_USAGE_STATS=false uv run pytest -m 'not integration'
uv run python scripts/build/build_artifacts.py                  # offline cache only
uv run python scripts/build/build_artifacts.py --unit glue/db-gb
uv run python scripts/build/smoke_artifacts.py                  # built units, local emulation
scripts/terraform_checks.sh                                   # isolated copy, mocked provider
```

## Safety

* No AWS, SSO, paid processing, apply/destroy, deploy or push without explicit authorisation.
  AWS, if later authorised: data-dev via SSO, sa-east-1, expected-account guard; never default
  (static keys) or data-prod. This migration authorises local work and a local commit only.
* Never run Terraform directly in the real dev directory: its ignored `.terraform/` retains the
  destroyed remote backend. Offline checks require a clean temporary copy and provider mocks.
* No secrets, passwords, `.env`, tfvars, state, plans or real/proprietary data in Git or evidence.
* Do not install/reconfigure Docker/WSL/system packages, remove others' volumes, or change the
  portfolio site repository. Preserve existing evidence; write new evidence to its own directory.
* Never claim Glue/Spark/Athena/AWS validation from local imports, moto or Terraform mocks.
