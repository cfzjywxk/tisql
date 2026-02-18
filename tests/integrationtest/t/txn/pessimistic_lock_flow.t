# Pessimistic transaction flow with mysql-test style sessions.
# Covers multi-session conflicts, rollback/commit, and uncommitted visibility.

CREATE TABLE lock_ctx_mtr (a INT PRIMARY KEY, b INT);
INSERT INTO lock_ctx_mtr VALUES (1, 10);

--session s1
BEGIN;
UPDATE lock_ctx_mtr SET b = 11 WHERE a = 1;

--session s2
BEGIN;
--error 1105
--error_kind KeyIsLocked
--error_contains locked
UPDATE lock_ctx_mtr SET b = 20 WHERE a = 1;

--session s1
ROLLBACK;

--session s2
UPDATE lock_ctx_mtr SET b = 20 WHERE a = 1;

--session s3
--rows 1
SELECT b FROM lock_ctx_mtr WHERE a = 1;

--session s2
COMMIT;

--session s3
--rows 1
SELECT b FROM lock_ctx_mtr WHERE a = 1;

--session s2
BEGIN;
INSERT INTO lock_ctx_mtr VALUES (2, 200);

--session s1
BEGIN;
--error 1105
--error_kind KeyIsLocked
--error_contains locked
INSERT INTO lock_ctx_mtr VALUES (2, 999);
--rows 0
SELECT a, b FROM lock_ctx_mtr WHERE a = 2;
ROLLBACK;

--session s2
ROLLBACK;

--session
SELECT a, b FROM lock_ctx_mtr ORDER BY a;
DROP TABLE lock_ctx_mtr;
