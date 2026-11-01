# Autonomous execution units

Migration baseline: independently approved `3e11db9` (H1/H2/H3). Business rules, CLI commands,
seven sources, source/date routing and the portfolio macro diagram are preserved. This layout
has not been deployed. Owner-selected Glue names were applied offline from `fec9280` without
functional changes; current evidence is `evidence/naming-sql/`. Earlier records remain intact.

Before:

```text
src/python/{pipeline,connect,extract,ingestion,transform,storage,load,sources,utils}
src/spark/{readers,transforms,writers}
src/{sql,contracts}
glue/{bronze/db_ingest.py,load/gold_load.py}     # one script served two Glue definitions
lambda/bronze/file_ingestion.py                # one ZIP served all three functions
```

After (only populated directories):

```text
apps/hybrid-runner/{erl.py,unit.json,python/,sql/extract/}
apps/publisher/{unit.json,requirements.txt,python/load/}     # own sql/ inside load package
apps/source-generator/{unit.json,requirements.txt,python/sources/}
glue/{extract-liquidez,extract-enquadramento,export-risk-analytics}/{job.py,spark/unit_spark.py,requirements.txt,unit.json}
lambda/market-data/{handler.py,python/market_data/,unit.json}
lambda/internal-xml/{handler.py,python/internal_xml/,unit.json}
lambda/external-xml/{handler.py,python/external_xml/,unit.json}
contracts/{bronze_contract.json,consumer_contract.json,*.xsd,source_schemas.py}
protocols/integrity-v2/erl_protocol/             # pinned 2.0.0 integrity exception
dbt/  airflow/  infra/  scripts/build/  tests/  evidence/  dist/ (ignored)
```

## Units and interfaces

| Unit | Public entry point / responsibility | Input → output | Artifact / resource |
|---|---|---|---|
| hybrid-runner | `erl` / `pipeline.cli:main`; configure, extract, coordinate, resume | sources/inbox + date/run → landing and step state/report | `apps/hybrid-runner/runner.zip`; local process, existing Airflow CLI calls |
| publisher | `load.package`, `load.publisher:publish` | passed gate + committed package → atomic consumer publication/log | `apps/publisher/publisher.zip`; local SQL Server consumer |
| source-generator | `sources.seed:generate_all` and scenario APIs | dates + caller's SourceEnvironment → synthetic databases/inbox | `apps/source-generator/source-generator.zip`; local fixtures |
| extract-liquidez | `glue/extract-liquidez/job.py` | landing manifests → committed Bronze snapshots | own `job.py` + `lib.zip`; `<prefix>-extract-liquidez` job/shared ingestion role |
| extract-enquadramento | `glue/extract-enquadramento/job.py` | landing manifests → partitioned committed Bronze | own `job.py` + `lib.zip`; `<prefix>-extract-enquadramento` job/shared ingestion role |
| export-risk-analytics | `glue/export-risk-analytics/job.py` | Iceberg Gold + passed gate → committed publication package | own `job.py` + `lib.zip`; `<prefix>-export-risk-analytics` job/export role |
| market-data | own `handler.handler`, public `market_data.parse` | CSV delivery/event → acceptance/quarantine manifest + Bronze | own `function.zip`; existing market Lambda/role |
| internal-xml | own `handler.handler`, public `internal_xml.parse` | internal XML → acceptance/quarantine manifest + Bronze | own `function.zip`; existing internal Lambda/role |
| external-xml | own `handler.handler`, public `external_xml.parse` | external XML → acceptance/quarantine manifest + Bronze | own `function.zip`; existing external Lambda/role |

Owner-selected names (renamed offline from `db-gb`, `db-tb`, `gold-load` respectively):
`extract-liquidez`, `extract-enquadramento`, `export-risk-analytics`. Naming does not redefine
business boundaries. The first still uses GB routing: fund_ops + portfolio_risk +
exposure_compliance instrument_registry/concentration_limit. The second still uses TB routing:
legacy_finance views + exposure_compliance stock_position. Both read landing snapshots, not
source databases directly. The exporter handles BOTH Gold products and does not publish to
SQL Server. The TB profile repartitions; this small lab has no TB-performance validation.
The two ingestion implementations remain independent even where bytes currently match.
The runner maps unchanged contract labels `glue_db_gb`/`glue_db_tb` explicitly to the chosen job
names (`GLUE_FOR_PATTERN`); profiles `gb`/`tb` and entity routing remain unchanged.

dbt continues to own Silver/Gold SQL and Airflow continues to call `erl step`. No new services,
commands, schedulers, Terraform state/bucket, or business functions were invented.

## Explicit dependencies, not global implementation

* Each `unit.json` declares responsibility, input/output interfaces (referencing the root contracts
  and protocol types), invariants, dependencies, external resources, state/effects and lifecycle.
  `public_dependencies` names APIs consumed and bundled by the unit (formerly the misleading
  `public_consumers`); wheel/runtime dependencies retain their existing fields.
* The runner consumes the publisher/generator public APIs and, in local mode, the three public
  parser APIs. Those declared integrations are bundled for a standalone local artifact. A parser
  change legitimately changes its own Lambda and the runner; other cloud units stay unchanged.
* Publisher and generator import without the runner. Publisher owns consumer SQL. The source
  generator receives connections through its small `SourceEnvironment` interface; source schema
  declarations moved once to contracts because extraction also needs their physical types.
* The integrity implementation is deliberately shared at **version 2.0.0**: canonical identity,
  manifest compatibility/commit/duplicate semantics, checksums, conditional writes/copies,
  codec proof, XML structural safety, and shared financial SQL types. These guarantees must not
  diverge across producers/readers. Parsers, Spark operations, source generation, orchestration
  and consumer publication are not in it. Version and selected module files are recorded per unit.
* It is source-vendored into each artifact, not loaded from a global mutable S3 ZIP. Updating the
  protocol requires tests of affected consumers and a compatibility decision: **2.0.0 is a semantic
  contract version**, not an implementation hash. Compatible internal edits can keep it; artifact
  SHA-256 identifies exact bytes. No additional version mechanism is needed. Copying business code
  into this exception is not the default for future units.
* Contracts have one source: root JSON/XSD/schema declarations and the pinned protocol's manifest
  types. SQL loaders bind to each owner's package; their common allowlist mechanism is versioned.

`objects.py` is actually imported by eight packages: runner, publisher, three Glue and three
Lambdas; the source generator does not consume it. A compatible internal edit therefore changes
eight packages legitimately, while Glue scripts and the generator stay identical. The import-closure
check finds no unused protocol modules in current packages. Critical hash/manifest/commit semantics
remain covered by behavior regressions, not by requiring an implementation hash to equal a version.

The GB/TB stage composition is intentionally owned twice. Conformance tests execute both real
job control flows with only Spark processing stubbed: bad checksum, write/copy failure, commit
after all parts, and completed-delivery reuse. These checks do not claim Spark execution.

## SQL ownership (54 versioned files, all retained byte-for-byte)

| SQL location | Executor / responsibility | Distribution |
|---|---|---|
| `apps/hybrid-runner/sql/extract/extract_query_table_*.sql`, `extract_query_view_*.sql` (10) | `pipeline.extraction_catalog` -> `extract.landing_snapshot`/`extract.sql_query`, source SELECTs | runner ZIP `sql/extract/`; installed wheel resource `sql` |
| Same folder: `readiness_query_table_processing_batch.sql` (1) | runner source readiness check | runner ZIP / wheel |
| Same folder: `extract_query_silver_rejected_records_summary.sql` (1) | `pipeline.steps.rejected_summary`, local DuckDB report read | runner ZIP / wheel; not Glue SQL |
| `apps/publisher/python/load/sql/export_query_gold_product.sql` (1) | `load.package.build_from_duckdb`, Gold -> publication package | publisher ZIP `load/sql/`; also bundled by its declared runner consumer |
| Same publisher folder: schema/lock/timeout SQL (5) | `load.publisher`, consumer schema setup and transaction locks | publisher ZIP / wheel; runner consumer bundles the same files |
| `dbt/{models,macros,tests}/` (36) | dbt adapter on DuckDB or Athena, transformation and quality | separate versioned dbt project, configured externally for the local runner; not bundled into cloud jobs |

No SQL move is warranted: ownership and loading already match execution. No Glue has SQL files
because its implementation uses native DataFrame expressions. All packaged SQL is checked with
`importlib.resources` from actual ZIPs; source bytes and archive members must match. Business SQL
is never copied into generic loaders. dbt/SQL content and published contracts remain unchanged.

## Build, Terraform and rollback

`build_artifacts.py --unit <runtime>/<unit>` builds only that declared unit; without `--unit` it
builds all. Before packaging, the explicit `--prepare-wheels` command may use the network to
prepare the target wheels: names come from unit declarations, versions from `uv.lock`, and the
same Python/platform/only-binary arguments are used as in the build. `uv sync` alone is not a
packaging-cache guarantee. The build remains unconditionally offline; CI also sets `UV_OFFLINE=true`
for build/smoke. Lambda owns pinned PyArrow; only XML Lambdas need lxml.
Local ZIPs require their declared external dependencies (the existing locked development env).
Each unit manifest records entry point, runtime, included files, dependency/protocol versions,
artifact SHA-256, S3 key, Git and uv.lock provenance. `smoke_artifacts.py` tests actual ZIPs in
separate `python -I -S` interpreters.

S3 artifact layout (future deployment, never uploaded by this migration):

```text
case01/lambda/<unit>/function.zip
case01/glue/<unit>/<script-content-sha256>/job.py
case01/glue/<unit>/<library-content-sha256>/lib.zip
```

Each Lambda uses its own object version. Each Glue definition names an immutable script/library
pair; SDK arguments come from the built unit manifest. This prevents mixed code versions during
an update. Deploy serially with jobs idle: Terraform can remove replaced content keys after the
job update, so old runs are not guaranteed continued access to removed keys. Content addressing
protects coherence, not availability of deleted artifacts. Rollback means rebuild/test the Git
commit and deploy again; it does not revert data, schemas or state.

One project/environment Terraform state remains. Glue job map keys/names and artifact prefixes
now follow the new unit IDs. `glue_workers` keys use these same IDs; worker counts and profiles
are unchanged. The exporter IAM role/policy has the corresponding name (`glue_gold_export`
Terraform address). Lambda/function/ingestion-IAM identifiers remain unchanged. If an old live
state were reused, renamed job keys and exporter IAM could plan replacement; no state was
migrated or queried here. The recorded lab is decommissioned: review a new authorised plan before
any future provisioning. No automatic state migration is supplied.

S3 objects now have per-unit ownership (six Glue objects and three Lambda objects instead of
one common library/script pair/package). Future init/plan needs renewed identity and explicit
approval; the destroyed backend and archived state must not be reused blindly. No state migration
or AWS action was performed here.

## Validation limits

Tests preserve H1/H2/H3, monetary precision, quarantine, idempotence, checkpoint ownership,
publication rollback and local dbt behavior. Build tests perturb one owner, add and remove units
in an isolated copy, compare unrelated artifacts, and reject missing declared consumers.
Cloud unit imports cannot resolve other units' private implementation. Smoke checks native
codecs/handlers with moto and import graphs with Spark/Glue stubs: no Spark/JVM, Glue, Athena,
Iceberg or AWS execution is claimed. Current results and artifact hashes: `evidence/naming-sql/` and ignored
`dist/manifest.json`.
