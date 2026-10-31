# Data model, contracts and rules

Everything here is **synthetic and authorial**: names, codes, amounts, limits and liquidity
factors were invented for this case. No rule below is a CVM, ANBIMA or any other regulatory
requirement.

## The seven sources

| # | Source (`source_system`) | Technology (local stand-in) | Entities extracted | Ingestion component (AWS design) |
|---|---|---|---|---|
| 1 | `fund_ops` – operational fund database | PostgreSQL db `fund_ops` | `fund`, `portfolio`, `source_identity_map` | `extract-liquidez` (GB profile) |
| 2 | `portfolio_risk` – portfolio risk database | PostgreSQL db `portfolio_risk` | `processed_position`, `processing_batch` | `extract-liquidez` (GB profile) |
| 3 | `legacy_finance` – legacy financial views | SQL Server db `legacy_finance` (views `vw_saldo_caixa`, `vw_resgate_pendente`) | `cash_balance`, `pending_redemption` | `extract-enquadramento` (TB profile) |
| 4 | `exposure_compliance` – exposure & compliance | SQL Server db `exposure_compliance` | `instrument_registry`, `stock_position`, `concentration_limit` | `extract-enquadramento` (stock / TB) / `extract-liquidez` (registers / GB) |
| 5 | `market_data` – daily external load | CSV `market_data_YYYYMMDD.csv` (`;`, decimal comma) | `daily_price` | Lambda *B3 diária* |
| 6 | `internal_xml` – internal portfolio XML | XML + XSD `internal_portfolio_statement.xsd` | `portfolio_position` | Lambda *XML internos* |
| 7 | `external_xml` – transfer-in XML | namespaced XML + XSD `transfer_in_portfolio.xsd` | `transfer_in_position` | Lambda *XML externos* |

The Glue names were selected by the owner. They retain the previous GB/TB routing above and
do not define exclusive liquidity/compliance domains; no entity mapping changed. Both ingest
landing snapshots, while local runner SQL extracts the source databases.

When no container runtime is available the four databases run on SQLite files with the same
SQLAlchemy table definitions and the same extraction SQL (`ERL_SOURCE_BACKEND=sqlite`). This is
a development stand-in and is labelled as such in every evidence file.

## Identifiers: one portfolio, five names

| System | Example for `PRT-0001` | Resolution |
|---|---|---|
| fund_ops (master) | `PRT-0001` | canonical id |
| portfolio_risk | `prt0001` | `upper(trim())` → `PRT0001` → `source_identity_map` |
| legacy_finance | `C000101` | `source_identity_map` |
| exposure_compliance | `FND001/01` | `source_identity_map` |
| external_xml | administrator `ADM77` + fund `EXT77-0001` | key `ADM77:EXT77-0001` → `source_identity_map` |

The crosswalk has validity dates (`valid_from`, `valid_to`) and is joined on the record's
reference date. A record whose identifier is not in the crosswalk is rejected as
`UNMAPPED_PORTFOLIO` (e.g. `prt0099` in the risk database).

Each portfolio has exactly one **system of record for positions**
(`portfolio.position_source_system`): PRT-0001…0005 → risk DB, PRT-0006 → internal XML,
PRT-0007/0008 → external transfer-in XML. Positions for a portfolio from any other system are
rejected as `NOT_SOURCE_OF_RECORD`.

## Layers

| Layer | Where | Content | Owner |
|---|---|---|---|
| Landing | `landing/db/…`, `landing/files/…` | extraction snapshots and delivered files, run-scoped keys | pipeline |
| Bronze raw | `bronze/raw/<source>/bronze_reference_date=D/<run_id>/<file>` | delivered bytes | pipeline / Lambda |
| Bronze structured | `bronze/structured/<source>/<entity>/bronze_reference_date=D/<run_id>*.parquet` | all business columns **as text, as received**, plus `meta_*` lineage columns | pipeline / Lambda / Glue |
| Manifests | `manifests/ingestion/…/<run_id>__<object>.json` | status, checksums, counts, rejection reason | same writer |
| Quarantine | `quarantine/<source>/bronze_reference_date=D/<run_id>/` | rejected file + `.rejection.json` | same writer |
| Silver | dbt (`silver` schema / Glue db) | typed, deduplicated, identity-resolved entities + `silver_rejected_records` | dbt |
| Gold | dbt (`gold` schema / Glue db) | `gold_liquidity_summary`, `gold_exposure_compliance` (+ audited `*_candidate`) | dbt |
| Publication package | `publish/reference_date=D/<run_id>/` | Parquet per product + `manifest.json` (written last) | Load step / Glue `export-risk-analytics` |
| Consumer | SQL Server `consumer_reporting` (`rpt.*`) | published products + `rpt.publication_log` | publisher |

Bronze metadata columns: `meta_source_system`, `meta_entity`, `meta_run_id`,
`meta_ingested_at`, `meta_component`, `meta_source_object`, `meta_source_sha256`,
`meta_snapshot_key`, `meta_row_number`, `meta_row_hash`. The single source of truth for
Bronze columns is `contracts/bronze_contract.json`; Terraform generates the Glue
Catalog Bronze tables from the same file.

### Snapshots and reprocessing

Bronze keeps every accepted delivery. A *snapshot key* names the unit that a newer delivery
replaces: one day of a database entity (`portfolio_risk/processed_position/2026-10-27`), one day
of market data, or one portfolio's XML statement for one day. Silver reads, for each snapshot
key, only the most recent delivery (`meta_ingested_at`, then `meta_run_id`). Reference
registers (funds, portfolios, crosswalk, instruments, limits) use the most recent delivery
overall.

Deduplication compares with the **current accepted snapshot**, not every historical hash.
Thus A → B → A accepts the final A as a legitimate correction. XML snapshots are compared
per portfolio; reference registers are compared across delivery dates.

Contract v2 represents an empty database snapshot with one metadata-only control row
(`meta_row_number = 0`, business columns NULL). Silver ranks deliveries before filtering
that row out, so deleting every source row clears the corresponding Silver snapshot.
Manifests continue to count business rows only. Existing nonempty v1 Bronze remains readable.
Old file manifests without `snapshot_key` may be accepted once again after this change.

An ACCEPTED ingestion manifest commits the entire delivery. Silver joins Bronze metadata to
`bronze.ingestion_deliveries` before choosing the latest snapshot; files left behind by a
failed ingestion cannot supersede an accepted delivery. Terraform declares this control table
with the Ion SerDe (existing multiline JSON); DuckDB reads the same manifest files directly.

Gold uses one MERGE per product to upsert the current keys and delete disappeared keys for
the processed date. Deletion markers exist only in the MERGE source and cannot be inserted
into Gold. Other dates are preserved. This is validated on DuckDB; Athena execution is pending.

Example: run 1 for D2 receives the PRT-0008 statement with an XSD error (quarantined). On D3 the
administrator retransmits the D2 statement; it is accepted into the **D2** partition and
flagged `late_arrival`. D2's Gold does not change until D2 is explicitly reprocessed; the
reprocess publishes a new version (the old one becomes `SUPERSEDED`).

## Record-level rejection reasons (Silver)

| Reason | Meaning | Example in the synthetic data |
|---|---|---|
| `INVALID_VALUE` | missing/non-numeric value, non-positive quantity, negative value | — (available for real feeds) |
| `UNMAPPED_PORTFOLIO` | identifier not in the crosswalk on that date | risk DB `prt0099` (2 rows per date) |
| `NOT_SOURCE_OF_RECORD` | positions from a system that is not the portfolio's system of record | — |
| `CONFLICTING_RECORDS` | same key and version with different values | — |
| `DUPLICATE_RECORD` | exact repetition of an already kept record | D2: repeated market data line; duplicated risk row |
| `MISSING_MARKET_DATA` | no valid price for the instrument on that date | degraded market data scenario |
| `UNKNOWN_INSTRUMENT` | compliance stock for an instrument outside the register | — |

Superseded processing versions (risk system reprocessing) are not rejections: Silver keeps the
highest `processing_version` (D2: PRT-0002 / SYN-EQ-005 v2 = 21 500 units instead of 20 000).

## Gold products and synthetic rules

### `gold_liquidity_summary` (one row per portfolio and reference date)

* `positions_value` = Σ market value of valid positions (system of record only)
* `cash_value` = legacy treasury balance (`vw_saldo_caixa`)
* `total_value` = positions + cash
* `liquid_value` = cash + Σ liquid part of each position, using `seeds/liquidity_rules.csv`:

| instrument_type | method | rule (synthetic) |
|---|---|---|
| GOV_BOND | FULL | 100 % of market value |
| CORP_BOND | FACTOR | 30 % of market value |
| FUND_QUOTA | FACTOR | 0 % |
| EQUITY | VOLUME | market value × min(1, 20 % × average daily volume ÷ quantity) |

  Worked example (PRT-0004, ISS-B equity): quantity 160 000, average daily volume 40 000 →
  sellable in one day = 0.20 × 40 000 = 8 000 units → liquid fraction 8 000 ÷ 160 000 = 5 %.
* `liquidity_ratio` = liquid ÷ total (0…1); `pending_redemptions` = open requests from
  `vw_resgate_pendente` (requested ≤ D < settlement); `redemption_coverage` = liquid ÷ pending.
* Lineage: `lineage_sources`, `lineage_bronze_runs`; `pipeline_run_id` in Gold.

### `gold_exposure_compliance` (one row per portfolio and reference date)

* exposure per position = compliance stock quantity × day's close price
* `top_issuer_pct` = largest issuer exposure ÷ total exposure, excluding issuers of type
  `SOVEREIGN` (configurable in `concentration_limit`)
* `concentration_check` = `BREACH` if `top_issuer_pct` > `concentration_limit_pct` (25 %,
  synthetic) else `PASS`. Only PRT-0004 breaches (ISS-B ≈ 60 %).
* `reconciliation_status`: compliance stock vs. positions from the system of record —
  `MATCHED`, `MISMATCH` (D3: PRT-0003 / SYN-GB-003 1 450 vs 1 500) or `NOT_AVAILABLE`.
* `quality_status` = `WARN` when the portfolio has mismatches, rejected records or no
  positions; otherwise `OK`.

## Quality gate

`dbt build` runs Silver tests and the **critical** tests on the two `*_candidate` models (write-
audit-publish). Both final Gold tables depend on both candidates, so a failure in either skips
both; the pipeline's `gate` step then refuses to load/publish.

| Critical test | Model |
|---|---|
| not empty, unique (reference_date, portfolio_id), not null keys, relationships to `silver_portfolio` | both candidates |
| `reconciles_liquidity_totals`: Gold total = independent Σ Silver positions + cash (exact decimals) | liquidity |
| `liquidity_ratio` in [0, 1]; `liquid_value <= total_value` | liquidity |
| `rejection_ratio_within`: rejected position-like records ≤ 15 % of the day | liquidity |
| `single_active_limit`, `top_issuer_pct` in [0, 1], accepted values | exposure |
| uniqueness/relationships/not-null/accepted values | Silver |

Warning (non-blocking): `assert_active_portfolios_have_positions` — reported in the run report.

## Consumer contract (`contracts/consumer_contract.json`)

Columns, logical types and keys of `rpt.liquidity_summary` and `rpt.exposure_compliance`
(plus `publication_id`). Money is `DECIMAL(20,2)`, ratios `DECIMAL(9,6)`; no binary floats
anywhere in the money path (extraction refuses `float`, the publisher refuses non-decimal Arrow
types).
