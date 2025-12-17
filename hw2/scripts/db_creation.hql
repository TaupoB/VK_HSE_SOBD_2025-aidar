CREATE DATABASE IF NOT EXISTS movielens;
USE movielens;

CREATE EXTERNAL TABLE IF NOT EXISTS movies (
    movieId INT,
    title STRING,
    genres STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://master:8020/user/hive/warehouse/movielens/movies'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS ratings (
    userId INT,
    movieId INT,
    rating FLOAT,
    tstamp STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://master:8020/user/hive/warehouse/movielens/ratings'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS tags (
    userId INT,
    movieId INT,
    tag STRING,
    tstamp STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://master:8020/user/hive/warehouse/movielens/tags'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS links (
    movieId INT,
    imdbId INT,
    tmdbId INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://master:8020/user/hive/warehouse/movielens/links'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS genome_scores (
    movieId INT,
    tagId INT,
    relevance FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://master:8020/user/hive/warehouse/movielens/genome_scores'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS genome_tags (
    tagId INT,
    tag STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://master:8020/user/hive/warehouse/movielens/genome_tags'
TBLPROPERTIES ("skip.header.line.count"="1");