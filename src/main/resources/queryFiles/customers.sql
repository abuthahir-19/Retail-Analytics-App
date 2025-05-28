CREATE TABLE CUSTOMERS (
    customer_id STRING,
    customer_unique_id  STRING,
    customer_zip_code_prefix INT,
    customer_city STRING,
    customer_state STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/data/olist_customers_dataset'
TBLPROPERTIES ("skip.header.line.count"="1");