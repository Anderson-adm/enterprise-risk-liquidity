-- portfolio_risk readiness: status of the latest batch of the reference date (NULL = none)
SELECT status
FROM processing_batch
WHERE reference_date = :reference_date
ORDER BY batch_id DESC
