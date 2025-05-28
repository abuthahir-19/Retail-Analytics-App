CREATE TABLE orders (
    order_id string,
    customer_id string,
    order_status string,
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/data/olist_orders_dataset'
TBLPROPERTIES ("skip.header.line.count"="1")