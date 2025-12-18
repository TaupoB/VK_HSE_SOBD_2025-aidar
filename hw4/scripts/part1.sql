
CREATE DATABASE IF NOT EXISTS demo_db;

USE demo_db;

CREATE TABLE IF NOT EXISTS test_events
(
    int_val   UInt32,
    uuid_val  UUID,
    dt_val    DateTime,
    str_val   String
)
ENGINE = MergeTree
ORDER BY (dt_val, int_val);


INSERT INTO test_events
SELECT
    q.int_val,
    q.uuid_val,
    q.dt_val,
    q.str_val
FROM
(
    SELECT
        modulo(rand(), 999) + 1 AS int_val,
        generateUUIDv4() AS uuid_val,
        now() - interval rand() / 1000 second AS dt_val,
        rand() / 500_000 AS int_val_2,
        multiIf(
            int_val_2 <= 1500, 'A',
            int_val_2 <= 3000, 'B',
            int_val_2 <= 4500, 'C',
            int_val_2 <= 6000, 'D',
            int_val_2 <= 7300, 'E',
            'F'
        ) AS str_val
    FROM numbers(10_000_000)
) q;


SELECT
    str_val,
    count()        AS total_rows,
    uniq(uuid_val) AS uniq_users
FROM test_events
GROUP BY str_val
ORDER BY total_rows DESC;


SELECT *
FROM system.clusters;


SELECT *
FROM system.macros;


SELECT *
FROM system.zookeeper
WHERE path = '/';


SELECT *
FROM system.distributed_ddl_queue;


SELECT *
FROM system.replication_queue;


SELECT *
FROM system.trace_log
LIMIT 100;

SELECT getMacro('cluster') AS cluster_macro;



SELECT
    hostName() AS host,
    count()    AS rows_cnt
FROM clusterAllReplicas('default', demo_db.test_events)
GROUP BY host;


SELECT
    query,
    query_duration_ms,
    memory_usage
FROM system.query_log
WHERE databases = ['demo_db']
ORDER BY event_time DESC
LIMIT 10;

SELECT
    table,
    sum(bytes_on_disk)               AS compressed_bytes,
    sum(data_uncompressed_bytes)     AS uncompressed_bytes,
    sum(primary_key_bytes_in_memory) AS primary_index_bytes
FROM system.parts
WHERE database = 'demo_db'
  AND table = 'test_events'
  AND active
GROUP BY table;


SELECT
    column,
    sum(column_data_compressed_bytes)   AS compressed_bytes,
    sum(column_data_uncompressed_bytes) AS uncompressed_bytes
FROM system.parts_columns
WHERE database = 'demo_db'
  AND table = 'test_events'
GROUP BY column
ORDER BY column;
