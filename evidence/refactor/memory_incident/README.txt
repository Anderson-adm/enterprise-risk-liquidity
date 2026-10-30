# First post-refactor SQLite evidence run (2026-10-30 ~15:27-15:41 local): 3 dbt builds failed with ENOMEM
# while DuckDB read manifests on /mnt/c. WSL swap was exhausted (2.0 GiB used, ~5 MiB free); SQL Server
# container ~1.3-1.6 GiB; concurrently this session ran pytest --collect-only x4 and a wheel venv install.
# Not a code defect: the containers run right after it and the isolated rerun are compared in ../comparison/.
# Action: concurrent work stopped; this project's containers stopped (volumes kept); SQLite rerun alone.
  IO Error: Could not read from file "<repo>/data/evidence-run-sqlite/lake/manifests/ingestion/market_data/daily_price/bronze_reference_date=2026-10-28/20261030T183045Z-8fafdb__market_data_20261028.csv.json": Cannot allocate memory
  IO Error: Could not read from file "<repo>/data/evidence-run-sqlite/lake/manifests/ingestion/portfolio_risk/processing_batch/bronze_reference_date=2026-10-27/20261030T183222Z-b995ec__20261030T183222Z-b995ec.parquet.json": Cannot allocate memory
  IO Error: Could not read from file "<repo>/data/evidence-run-sqlite/lake/manifests/ingestion/portfolio_risk/processing_batch/bronze_reference_date=2026-10-28/20261030T183649Z-57375d__20261030T183649Z-57375d.parquet.json": Cannot allocate memory
