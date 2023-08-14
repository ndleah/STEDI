-- Create an external table named 'customer_landing' in the 'stedi-data-lake' database
-- This table is meant to hold customer data from a data lake.

CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-data-lake`.`customer_trusted` (
    -- Define the columns for the table
    `customerName`              string,
    `email`                     string,
    `phone`                     string,
    `birthday`                  string,
    `serialNumber`              string,
    `registrationDate`          bigint,
    `lastUpdateDate`            bigint,
    `shareWithResearchAsOfDate` bigint,
    `shareWithPublicAsOfDate`   bigint,
    `shareWithFriendsAsOfDate`  bigint
)
-- Specify that the data is in JSON format using the 'JsonSerDe' serde (serializer/deserializer)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
-- Set serde-specific properties to handle JSON data
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'FALSE', -- Don't ignore malformed JSON data
    'dots.in.keys' = 'FALSE', -- Don't allow dots in keys
    'case.insensitive' = 'TRUE', -- Case-insensitive keys
    'mapping' = 'TRUE' -- Treat data as a key-value mapping
)
-- Specify the input and output formats for reading and writing data
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
-- Set the location in the data lake where the table's data is stored
LOCATION 's3://stedi-data-lake/project/customer/trusted/'
-- Set additional table properties
TBLPROPERTIES ('classification' = 'json');
