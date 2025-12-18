CREATE DATABASE IF NOT EXISTS demo_db;

CREATE TABLE IF NOT EXISTS demo_db.test_events_direct
(
    int_val   UInt32,
    uuid_val  UUID,
    dt_val    DateTime,
    str_val   String
)
ENGINE = MergeTree
ORDER BY (dt_val, int_val);

CREATE TABLE IF NOT EXISTS demo_db.test_events_target
(
    int_val   UInt32,
    uuid_val  UUID,
    dt_val    DateTime,
    str_val   String
)
ENGINE = MergeTree
ORDER BY (dt_val, int_val);


CREATE TABLE IF NOT EXISTS demo_db.test_events_buffer
AS demo_db.test_events_target
ENGINE = Buffer(demo_db, test_events_target, 16, 5, 10, 1000, 10000, 1000000, 10000000);



SELECT 
    'Direct Table' as source, count() as cnt FROM demo_db.test_events_direct
UNION ALL
SELECT 
    'Target Table' as source, count() as cnt FROM demo_db.test_events_target;

SELECT 
    table, 
    count() AS parts_count, 
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts 
WHERE database = 'demo_db' 
  AND table IN ('test_events_direct', 'test_events_target') 
  AND active = 1 
GROUP BY table;