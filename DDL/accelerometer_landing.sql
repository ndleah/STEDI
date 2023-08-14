-- Create an external table named 'accelerometer_landing' in the database 'ab-udacity-project'
-- If the table already exists, this command won't create a new one
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-data-lake`.`accelerometer_landing` (
    -- Define columns in the table: 'user', 'timeStamp', 'x', 'y', and 'z'
    `user`      string,
    `timeStamp` bigint,
    `x`         float,
    `y`         float,
    `z`         float
)
-- Specify that the table uses 'JsonSerDe' for JSON serialization and deserialization
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
-- Set SERDEPROPERTIES for JSON handling
WITH SERDEPROPERTIES (
    -- Treat malformed JSON as valid (set to 'FALSE' if you want to reject malformed JSON)
    'ignore.malformed.json' = 'FALSE',
    -- Allow dots in keys (set to 'TRUE' if your JSON keys have dots)
    'dots.in.keys' = 'FALSE',
    -- Perform case-insensitive matching for keys (set to 'FALSE' for case-sensitive)
    'case.insensitive' = 'TRUE',
    -- Use mapping for struct and map data types (set to 'FALSE' if not needed)
    'mapping' = 'TRUE'
)
-- Specify input and output formats for data processing
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
-- Specify the location where the data files are stored in Amazon S3
LOCATION 's3://stedi-data-lake/project/accelerometer/landing/'
-- Set table properties, including classification of data as JSON
TBLPROPERTIES ('classification' = 'json');
