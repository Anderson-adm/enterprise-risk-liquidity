-- DuckDB warehouse: rejected records of the reference date, for the run report
SELECT entity, source_system, reject_reason, count(*)
FROM silver.silver_rejected_records
WHERE reference_date = $reference_date
GROUP BY ALL
ORDER BY ALL
