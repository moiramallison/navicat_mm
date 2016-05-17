select pid, query from pg_stat_activity
WHERE state = 'active'



SELECT pg_cancel_backend(8199);
