-- DuckDB Gold -> publication package: rows written by this pipeline run only.
-- {columns} and {gold_model} come from consumer_contract.json and are validated before rendering.
SELECT {columns}
FROM gold.{gold_model}
WHERE reference_date = $reference_date AND pipeline_run_id = $pipeline_run_id
ORDER BY portfolio_id
