DROP TABLE IF EXISTS t03;
CREATE TABLE t03 (id int, val int);
INSERT INTO t03 (id, val) SELECT i, i FROM generate_series(1, 100) i;

-- gang creation order is important, reset any guc to force the creation
10: RESET optimizer;
20: RESET optimizer;
30: RESET optimizer;

10: BEGIN;
20: BEGIN;
30: BEGIN;

10: UPDATE t03 SET val=val WHERE id IN (segid(0,2), segid(1,2));

20: UPDATE t03 SET val=val WHERE id=segid(0,1);

30: UPDATE t03 SET val=val WHERE id=segid(1,1);

-- seg 0: con10 ==> con20, xid lock
-- seg 1: con10 ==> con30, xid lock
10&: UPDATE t03 SET val=val WHERE id IN (segid(0,1), segid(1,1));

-- seg 1: con30 ==> con10, xid lock
30>: UPDATE t03 SET val=val WHERE id=segid(1,2);

-- a local deadlock between con10 & con30 is formed on seg1,
-- con10 will be cancelled by local deadlock detector.
-- note, we must join c20 before c10, otherwise the test will run into infinite
-- waiting if the local deadlock detector cancels c30 instead of c10.
30<:
30q:
20<:
20q:
10<:
10q:
