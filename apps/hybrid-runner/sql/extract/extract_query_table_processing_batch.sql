-- portfolio_risk.processing_batch: batches of the reference date
SELECT batch_id, reference_date, status, started_at, finished_at
FROM processing_batch
WHERE reference_date = :reference_date
ORDER BY batch_id
