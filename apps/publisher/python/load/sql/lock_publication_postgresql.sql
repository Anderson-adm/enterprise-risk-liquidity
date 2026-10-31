-- Transaction-scoped advisory lock: serialises publications (released at commit/rollback).
SELECT pg_advisory_xact_lock(:lock_key)
