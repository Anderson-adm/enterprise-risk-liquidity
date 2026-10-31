# Autonomous execution units

Migration baseline: independently approved `3e11db9` (H1/H2/H3). Business rules, CLI commands,
seven sources, source/date routing and the portfolio macro diagram are preserved. This layout
has not been deployed. Earlier refactors and evidence remain in Git history/evidence/refactor.

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
glue/{db-gb,db-tb,gold-load}/{job.py,spark/unit_spark.py,requirements.txt,unit.json}
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
| db-gb | `glue/db-gb/job.py` | landing manifests → committed Bronze snapshots | own `job.py` + `lib.zip`; existing `*-db-gb` job/ingestion role |
| db-tb | `glue/db-tb/job.py` | landing manifests → partitioned committed Bronze | own `job.py` + `lib.zip`; existing `*-db-tb` job/ingestion role |
| gold-load | `glue/gold-load/job.py` | Iceberg Gold + passed gate → committed publication package | own `job.py` + `lib.zip`; existing Gold load job/role |
| market-data | own `handler.handler`, public `market_data.parse` | CSV delivery/event → acceptance/quarantine manifest + Bronze | own `function.zip`; existing market Lambda/role |
| internal-xml | own `handler.handler`, public `internal_xml.parse` | internal XML → acceptance/quarantine manifest + Bronze | own `function.zip`; existing internal Lambda/role |
| external-xml | own `handler.handler`, public `external_xml.parse` | external XML → acceptance/quarantine manifest + Bronze | own `function.zip`; existing external Lambda/role |

`db-gb` / `db-tb` are volume patterns, not liquidity/exposure products. The contract still routes
fund_ops and portfolio_risk to GB; legacy_finance to TB; exposure_compliance uses both. Their
initial scripts/libraries can have identical bytes; source ownership and S3 prefixes are separate.
The TB profile repartitions; this small lab has no TB-performance validation.

dbt continues to own Silver/Gold SQL and Airflow continues to call `erl step`. No new services,
commands, schedulers, Terraform state/bucket, or business functions were invented.

## Explicit dependencies, not global implementation

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
  protocol requires an explicit version decision and tests of affected consumers. Copying business
  code into this exception is not the default for future units.
* Contracts have one source: root JSON/XSD/schema declarations and the pinned protocol's manifest
  types. SQL loaders bind to each owner's package; their common allowlist mechanism is versioned.

The GB/TB stage composition is intentionally owned twice. Conformance tests execute both real
job control flows with only Spark processing stubbed: bad checksum, write/copy failure, commit
after all parts, and completed-delivery reuse. These checks do not claim Spark execution.

## Build, Terraform and rollback

`build_artifacts.py --unit <runtime>/<unit>` builds only that declared unit; without `--unit` it
builds all. Dependencies come from the existing uv cache with unconditional `--offline`; no
installation or download is implicit. Lambda owns pinned PyArrow; only XML Lambdas need lxml.
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

One project/environment Terraform state remains. Existing job/function/IAM identifiers remain;
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
Iceberg or AWS execution is claimed. Results and artifact hashes: `evidence/units/` and ignored
`dist/manifest.json`.
