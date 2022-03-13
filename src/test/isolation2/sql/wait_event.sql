-- test WaitEvent related logic, the specific event will be watched in pg_stat_activity

create table test_waitevent(i int);
insert into test_waitevent select generate_series(1,1000);
create extension if not exists gp_inject_fault;

1: set optimizer = off;
1: set gp_cte_sharing to on;
1: select gp_inject_fault_infinite('shareinput_writer_notifyready', 'suspend', 2);
1&: WITH a1 as (select * from test_waitevent), a2 as (select * from test_waitevent) SELECT sum(a1.i)  FROM a1 INNER JOIN a2 ON a2.i = a1.i  UNION ALL SELECT count(a1.i)  FROM a1 INNER JOIN a2 ON a2.i = a1.i;
-- start_ignore
-- query pg_stat_get_activity on segment to watch the ShareInputScan event
2: copy (select pg_stat_get_activity(NULL) from gp_dist_random('gp_id') where gp_segment_id=0) to '/tmp/_gpdb_test_output.txt';
-- end_ignore
2: select gp_wait_until_triggered_fault('shareinput_writer_notifyready', 1, 2);
2: select gp_inject_fault_infinite('shareinput_writer_notifyready', 'resume', 2);
2: select gp_inject_fault_infinite('shareinput_writer_notifyready', 'reset', 2);
2q:
1<:
1q:

!\retcode grep ShareInputScan /tmp/_gpdb_test_output.txt;

