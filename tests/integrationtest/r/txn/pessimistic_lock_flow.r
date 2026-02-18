CREATE TABLE lock_ctx_mtr (a INT PRIMARY KEY, b INT)
INSERT INTO lock_ctx_mtr VALUES (1, 10)
AFFECTED 1
BEGIN
UPDATE lock_ctx_mtr SET b = 11 WHERE a = 1
AFFECTED 1
BEGIN
UPDATE lock_ctx_mtr SET b = 20 WHERE a = 1
ERROR 1105: KeyIsLocked
ROLLBACK
UPDATE lock_ctx_mtr SET b = 20 WHERE a = 1
AFFECTED 1
SELECT b FROM lock_ctx_mtr WHERE a = 1
b
10
COMMIT
SELECT b FROM lock_ctx_mtr WHERE a = 1
b
20
BEGIN
INSERT INTO lock_ctx_mtr VALUES (2, 200)
AFFECTED 1
BEGIN
INSERT INTO lock_ctx_mtr VALUES (2, 999)
ERROR 1105: KeyIsLocked
SELECT a, b FROM lock_ctx_mtr WHERE a = 2
a	b
ROLLBACK
ROLLBACK
SELECT a, b FROM lock_ctx_mtr ORDER BY a
a	b
1	20
DROP TABLE lock_ctx_mtr
