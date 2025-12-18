DROP TABLE IF EXISTS default.person_data;

CREATE TABLE default.person_data (
  id          UInt64,
  region      LowCardinality(String),
  date_birth  Date,
  gender      UInt8,
  is_marital  UInt8,
  dt_create   DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (date_birth);

INSERT INTO default.person_data(id, region, date_birth, gender, is_marital)
SELECT 
    q.id, 
    q.region, 
    toStartOfDay(q.date_birth) AS date_birth, 
    q.gender, 
    q.is_marital
FROM (
    SELECT 
       rand() AS id,
       modulo(id, 70) + 20 AS n,
       toString(n) AS region,
       floor(randNormal(10000, 1700)) AS k,
       toDate('1970-01-01') + interval k day AS date_birth,
       if(modulo(id, 3) = 1, 1, 0) AS gender,
       if((n + k) % 3 = 0 AND date_diff('year', date_birth, now()) > 18, 1, 0) AS is_marital
    FROM numbers(100000000)
) q;

OPTIMIZE TABLE default.person_data FINAL;


SELECT t.region,
       countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
       countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
  FROM default.person_data t
 WHERE t.date_birth BETWEEN toDate('2000-01-01') AND toDate('2000-01-31')
   AND t.region IN ('20', '25', '43', '59')
 GROUP BY t.region; 

SELECT countIf(gender = 1 AND date_diff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
       countIf(gender = 0 AND date_diff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
  FROM default.person_data t
 WHERE t.is_marital = 1
   AND t.region IN ('80')
 GROUP BY t.region;


SELECT 
    table,
    formatReadableSize(sum(primary_key_bytes_in_memory)) as pk_size,
    sum(rows) as rows_count,
    count() as parts_count
FROM system.parts
WHERE  active = 1
GROUP BY table;


DROP TABLE IF EXISTS default.person_data_optimized;

CREATE TABLE default.person_data_optimized (
  id          UInt64,
  region      LowCardinality(String),
  date_birth  Date,
  gender      UInt8,
  is_marital  UInt8,
  dt_create   DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (region, date_birth);

INSERT INTO default.person_data_optimized SELECT * FROM default.person_data;
OPTIMIZE TABLE default.person_data_optimized FINAL;


DROP TABLE IF EXISTS default.person_data_compressed;

CREATE TABLE default.person_data_compressed 
(
    id UInt64 CODEC(ZSTD(3)),
    region LowCardinality(String) CODEC(ZSTD(3)),
    date_birth Date CODEC(Delta, ZSTD(3)),
    gender UInt8 CODEC(ZSTD(3)),
    is_marital UInt8 CODEC(ZSTD(3)),
    dt_create DateTime DEFAULT now() CODEC(Delta, ZSTD(3))
)
ENGINE = MergeTree()
ORDER BY (region, date_birth);

INSERT INTO default.person_data_compressed 
SELECT * FROM default.person_data;

OPTIMIZE TABLE default.person_data_compressed FINAL;

SELECT 
    table, 
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) as original_size,
    round((1 - sum(data_compressed_bytes) / sum(data_uncompressed_bytes)) * 100, 2) as compression_rate_percent
FROM system.parts
WHERE table IN ('person_data', 'person_data_compressed') 
  AND active = 1
GROUP BY table;


