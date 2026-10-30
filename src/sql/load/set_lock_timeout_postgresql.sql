-- Bound the wait for the publication lock in this transaction only.
SELECT set_config('lock_timeout', :timeout, true)
