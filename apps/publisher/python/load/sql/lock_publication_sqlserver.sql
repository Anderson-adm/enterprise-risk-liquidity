-- Transaction-scoped exclusive application lock: serialises publications (released at commit/rollback).
-- Returns the sp_getapplock return code: >= 0 acquired, < 0 timeout/deadlock/error.
DECLARE @rc int;
EXEC @rc = sp_getapplock @Resource = :resource, @LockMode = 'Exclusive', @LockOwner = 'Transaction',
     @LockTimeout = :timeout_ms;
SELECT @rc
