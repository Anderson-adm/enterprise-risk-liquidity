# Local runtime and preflight

## Preflight of 2026-10-29 (read-only diagnostics, nothing changed)

| Item | Found |
|---|---|
| OS | Windows 11 + WSL2, Ubuntu 26.04.1 LTS, kernel 6.18 |
| CPU / RAM | 12 threads; WSL2 VM 7.7 GiB (6.4 GiB available), 2 GiB swap |
| Disk | WSL root 949 GB free; `C:` 63 GB free (repo lives on `/mnt/c`) |
| Python | system 3.14.4; project uses **3.12.14** installed by uv (dbt-athena supports ≤ 3.13; Lambda 3.12; Glue 5.0 is 3.11, so code targets 3.11+) |
| uv | 0.12.17 |
| Terraform | 1.16.6 |
| AWS CLI | 2.37.4; profiles `data-dev`, `data-prod` (SSO, token expired), `default` (**static keys**) |
| Docker | Docker Desktop installed under `AppData/Local/Programs/DockerDesktop`, CLI 29.8.0, **engine not running**, WSL integration disabled |
| Not installed (WSL) | tflint (downloaded to `.tools/bin`, checksum-verified), checkov (run via `uvx`), gh (only a Windows `gh.exe`), Java, sqlcmd/psql, ODBC drivers |

At that first preflight no container runtime was running, so development started on **SQLite
stand-ins** for the four source databases and the consumer, through the same SQLAlchemy code
(`ERL_SOURCE_BACKEND=sqlite`). SQL Server access uses `pymssql` (wheels bundle FreeTDS), so no
ODBC system packages are needed. Airflow runs in its own venv (`airflow/.venv`) with SQLite
metadata — no containers.

## Containers run of 2026-10-29 (authorised by the owner)

| Item | Found / done |
|---|---|
| Docker | Docker Desktop already running with WSL integration for `Ubuntu` enabled (`/usr/bin/docker` → Desktop CLI); engine 29.8.0. Nothing installed or reconfigured; `.wslconfig` not touched. |
| Resources before start | WSL2 VM 7.7 GiB total, 6.2 GiB available, swap 2 GiB unused; WSL disk 948 GB free, `C:` 57 GB free |
| Pre-existing Docker objects | 3 volumes of other projects, 1 image, no containers — left untouched. This project only uses the named volumes `erl_pgdata` and `erl_mssqldata`. |
| Start order | PostgreSQL first (`up -d --wait postgres`, healthy), then SQL Server (healthy after ~18 s) |
| Tests / evidence databases | `it_*` (pytest) and `ev_*` (evidence script) on the same servers; the development databases are not touched |
| Airflow | not run together with the databases (not needed for these checks) |

Measured memory (`docker stats`), never near the limits:

| Container | Limit | Idle | Peak observed (8-scenario evidence run) |
|---|---|---|---|
| PostgreSQL 17.11 (alpine) | 384 MiB, 1 CPU | ~25–30 MiB | ~97 MiB |
| SQL Server 2022 CU27 Developer | 2 GiB, 2 CPU (`MSSQL_MEMORY_LIMIT_MB=1536`) | ~625 MiB | ~1.14 GiB |

The 2.4 GiB aggregate is **not** shared by SQL Server: SQL Server alone has 2 GiB (its
documented minimum) and PostgreSQL its own 384 MiB. With both running plus dbt/pytest, the VM
still had ~4.5 GiB available, so the limits were kept as they were.

## Option A — no containers (works today)

```bash
uv sync
uv run erl generate
uv run erl run --date 2026-10-26
```

## Option B — real PostgreSQL + SQL Server (requires a running Docker engine)

Recommended runtime: **Docker Desktop with WSL integration enabled for this distro** (already
installed; enabling integration is a Docker Desktop setting the user must change). Then:

```bash
cp .env.example .env    # set ERL_SOURCE_BACKEND=containers and both local passwords
set -a; . ./.env; set +a
docker compose -f docker/compose.yaml --env-file .env up -d --wait postgres
docker compose -f docker/compose.yaml --env-file .env up -d --wait sqlserver
docker stats --no-stream
uv run erl generate      # creates the 5 logical databases and loads the synthetic data
uv run erl run --date 2026-10-26
ERL_IT_CONTAINERS=1 uv run pytest -m integration          # it_* databases
uv run python scripts/collect_evidence.py                 # ev_* databases -> evidence/local/containers
uv run python scripts/collect_db_evidence.py              # -> evidence/databases/*.json
docker compose -f docker/compose.yaml down                # stops; keeps the named volumes
```

If Docker Desktop runs without WSL integration, published ports are on the Windows host; set
`ERL_PG_HOST` / `ERL_MSSQL_HOST` to the Windows host address.
