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
| Not installed | tflint (downloaded to `.tools/bin`, checksum-verified), checkov (run via `uvx`), gh, Java, sqlcmd/psql, ODBC drivers |

Consequences:

* PostgreSQL and SQL Server containers could not be started (starting a container runtime
  requires authorisation). Development and tests use **SQLite stand-ins** for the four source
  databases and the consumer, through the same SQLAlchemy code (`ERL_SOURCE_BACKEND=sqlite`).
* SQL Server access uses `pymssql` (wheels bundle FreeTDS), so no ODBC system packages are
  needed.
* Airflow runs in its own venv (`airflow/.venv`) with SQLite metadata — no containers.

## Memory budget with containers (when authorised)

| Component | Limit | Note |
|---|---|---|
| PostgreSQL 17 (alpine) | 384 MB, 1 CPU | two logical databases |
| SQL Server 2022 Developer | 2 GB, 2 CPU (`MSSQL_MEMORY_LIMIT_MB=1536`) | three logical databases; SQL Server requires ≥ 2 GB |
| Airflow (local venv, `dags test`) | not measured | runs only when testing the DAG |
| dbt + DuckDB | not measured (kilobytes of data) | sequential (`threads: 1`) |

Total stays below ~4 GB of the 6.4 GiB available in WSL2. Run services only when needed and
stop them afterwards (`docker compose -f docker/compose.yaml down`).

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
docker compose -f docker/compose.yaml up -d --wait
uv run erl generate      # creates the 5 logical databases and loads the synthetic data
uv run erl run --date 2026-10-26
ERL_IT_CONTAINERS=1 uv run pytest -m integration
docker compose -f docker/compose.yaml down
```

If Docker Desktop runs without WSL integration, published ports are on the Windows host; set
`ERL_PG_HOST` / `ERL_MSSQL_HOST` to the Windows host address.
