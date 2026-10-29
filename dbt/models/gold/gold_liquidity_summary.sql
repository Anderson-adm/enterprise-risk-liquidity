{{ gold_snapshot_config() }}
-- depends_on: {{ ref('gold_exposure_compliance_candidate') }}
-- The explicit dependency on the *other* product's candidate makes the reference date's
-- critical tests one gate: if either candidate fails, dbt build skips BOTH Gold tables.
-- Upserts current keys and deletes vanished keys atomically; other dates are untouched.
{{ gold_snapshot(ref('gold_liquidity_summary_candidate')) }}
