CREATE TABLE retail_analytics.filtered_orders (
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp DateTime,
    order_approved_at DateTime,
    order_delivered_carrier_date DateTime,
    order_delivered_customer_date DateTime,
    order_estimated_delivered_date DateTime
) ENGINE = MergeTree()
ORDER BY order_id;

CREATE TABLE retail_analytics.enriched_orders_items (
    order_id String,
    product_id String,
    price Float64,
    customer_id String,
    customer_city String,
    customer_state String,
    seller_id String,
    seller_city String,
    seller_state String
) ENGINE = MergeTree()
ORDER BY order_id;


CREATE TABLE retail_analytics.customer_score_data (
    customer_id String,
    order_purchase_hourly_window String,
    Total_Order_Value Float64
) ENGINE = MergeTree()
ORDER BY Total_Order_Value;

CREATE TABLE retail_analytics.delivery_stat (
        order_id String,
        customer_id String,
        order_status String,
        order_purchase_timestamp DateTime,
        order_approved_at DateTime,
        order_delivered_carrier_date DateTime,
        order_delivered_customer_date DateTime,
        order_estimated_delivered_date DateTime,
        Delivery_delay Int64,
        Is_Late String
) ENGINE = MergeTree()
ORDER BY order_id;

CREATE TABLE retail_analytics.high_value_order_stat (
        order_id String,
        payment_type String,
        payment_value Float64,
        rank Int64
) ENGINE = MergeTree()
ORDER BY order_id;

CREATE TABLE retail_analytics.top_selling_product_category (
        product_category_name String,
        window String,
        Total_Selling_Value Float64
) ENGINE = MergeTree()
ORDER BY product_category_name;

CREATE TABLE retail_analytics.credit_card_dscns (
        order_id String,
        price Float64,
        payment_type String,
        discount_price Float64
) ENGINE = MergeTree()
ORDER BY product_category_name;

CREATE TABLE retail_analytics.order_pymnts_dedup (
        order_id String,
        payment_sequential Int64,
        payment_value Float64
) ENGINE = MergeTree()
ORDER BY product_category_name;

CREATE TABLE default.products_cleaned (
   product_id String,
   product_category_name String,
   product_name_length Int,
   product_description_length Int,
   product_photos_qty Int,
   product_weight_g Int,
   product_length_cm Int,
   product_height_cm Int,
   product_width_cm Int
) ENGINE = MergeTree()
ORDER BY product_id;