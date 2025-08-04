CREATE TABLE `all_usage`
(
    `a_number`    string,
    `b_number`    string,
    `duration`    int,
    `cdr_type`    string,
    `imei`        string,
    `imsi`        string,
    `ms_location` string,
    `usage`       bigint,
    `rat_id`      string,
    `start_time`  string
)
    PARTITIONED BY (
    `start_date` int)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
    'compression' = 'zstd',
    'path' = 'hdfs:///warehouse/tablespace/managed/hive/all_usage')
STORED AS PARQUET
LOCATION
    'hdfs:///warehouse/tablespace/managed/hive/all_usagez'
TBLPROPERTIES (
    'bucketing.version' = '2',
    'spark.sql.create.version' = '3.2.3',
    'spark.sql.partitionProvider' = 'catalog',
    'spark.sql.sources.provider' = 'parquet',
    'spark.sql.sources.schema' =
            '{"type":"struct","fields":[{"name":"a_number","type":"string","nullable":true,"metadata":{}},{"name":"b_number","type":"string","nullable":true,"metadata":{}},{"name":"duration","type":"integer","nullable":true,"metadata":{}},{"name":"cdr_type","type":"string","nullable":true,"metadata":{}},{"name":"imei","type":"string","nullable":true,"metadata":{}},{"name":"imsi","type":"string","nullable":true,"metadata":{}},{"name":"ms_location","type":"string","nullable":true,"metadata":{}},{"name":"usage","type":"long","nullable":true,"metadata":{}},{"name":"rat_id","type":"string","nullable":true,"metadata":{}},{"name":"start_time","type":"string","nullable":true,"metadata":{}},{"name":"start_date","type":"integer","nullable":true,"metadata":{}}]}',
    'spark.sql.sources.schema.numPartCols' = '1',
    'spark.sql.sources.schema.partCol.0' = 'start_date'
    );