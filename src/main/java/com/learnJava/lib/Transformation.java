package com.learnJava.lib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.Window;

import java.util.Arrays;

public class Transformation {
    private static final Logger LOG = LogManager.getLogger();
    private static SparkSession spark;
    public Transformation (SparkSession sparkSession) {
        spark = sparkSession;
    }
    // Filter out the order which is of cancelled or unavailable

    public Dataset<Row> FilterOrders (Dataset<Row> orders) {
        try {
            assert orders != null;
            return orders
                    .filter (col ("order_status").isInCollection(Arrays.asList ("shipped", "approved", "invoiced", "created", "delivered", "processing")));
        } catch (Exception e) {
            LOG.info ("Exception while reading the orders data !!");
        }
        return null;
    }

    public Dataset<Row> DoDataEnrichment (Dataset <Row> orderItems, Dataset<Row> orders, Dataset<Row> customers, Dataset<Row> sellers) {
        LOG.info ("Data enrichment process on-going");
        assert orderItems != null;
        return orderItems.join (orders, "order_id")
                .join (customers, "customer_id")
                .join (sellers, "seller_id")
                .select ("order_id", "product_id", "price", "customer_id", "customer_city", "customer_state", "seller_id", "seller_city", "seller_state");
    }

    // Compute the total order value per customer to identify the high value customers
    public Dataset<Row> GetCustomerScore (Dataset<Row> orderItems, Dataset<Row> orders) {
        assert orderItems != null;
        return orderItems
                .join (orders, "order_id")
                .groupBy (
                        col ("customer_id"),
                        window (col("order_purchase_timestamp"), "1 hour")
                )
                .agg (round(sum(col ("price").plus(col ("freight_value"))), 2).alias ("Total_Order_Value"))
                .orderBy (col ("Total_Order_Value").desc());
    }

    // To evaluate the performance of the logistics service partner (Which tells the order and its respective delay)
    public Dataset <Row> CalculateDeliveryDelay (Dataset<Row> orders) {
        assert orders != null;
        return orders
                .filter (col ("order_delivered_customer_date").isNotNull().and(col ("order_estimated_delivery_date").isNotNull()))
                .withColumn ("Delivery_delay", datediff (col ("order_estimated_delivery_date"), col ("order_delivered_customer_date")))
                .withColumn ("Is_Late", when(col ("Delivery_delay").gt(0), "Yes").otherwise("No"));
    }

    // To predict the fraudulent transactions like high value orders for example (Top 10 high value orders in each payment_type)
    public Dataset <Row> FlagHighValueOrders (Dataset<Row> orderPayments) {
        assert orderPayments != null;

        LOG.info ("Getting information of unique payment types");
        orderPayments.
                groupBy (col ("payment_type"))
                .count().alias ("Total Transactions")
                .show(false);

        return orderPayments
                .filter (col ("payment_type").notEqual("not_defined"))
                .withColumn ("rank", dense_rank().over(Window.partitionBy ("payment_type").orderBy (col("payment_value").desc())))
                .filter (col ("rank").leq (10))
                .select ("order_id", "payment_type", "payment_value", "rank");
    }

    // Get the Top selling product category
    public Dataset<Row> PredictTopSellingProductCategory (Dataset<Row> orderItems, Dataset<Row> products, Dataset<Row> orders) {
        assert orderItems != null;

        LOG.info ("Getting the top selling product category in an hourly window");
        return orderItems
                .join (products, "product_id")
                .join (orders, "order_id")
                .groupBy (
                        col ("product_category_name"),
                        window (col("order_purchase_timestamp"), "1 hour")
                )
                .agg(round(sum (col ("price")), 2).alias ("Total_Selling_Value"))
                .filter (col ("product_category_name").notEqual("NULL"))
                .orderBy(col ("product_category_name"));
    }

    // Apply discounts for credit card payments
    public Dataset <Row> ApplyDiscountsForCreditCard (Dataset<Row> orderPayments, Dataset<Row> orderItems) {
        assert orderPayments != null;
        assert  orderItems != null;

        return orderItems
                .join (orderPayments, "order_id")
                .withColumn ("discount_price", when (col ("payment_type").equalTo("credit_card"), round(col ("price").multiply(0.95), 2)).otherwise(col ("price")))
                .select ("order_id", "price", "payment_type", "discount_price");
    }

    // Data deduplication for order payment to avoid invalid revenue report
    public Dataset<Row> DataDeduplication (Dataset<Row> orderPayments) {
        assert orderPayments != null;

        LOG.info ("Analysing the order payments data...");
        orderPayments
                .groupBy ("order_id", "payment_sequential", "payment_value")
                .count().alias ("Total Count")
                .show(false);

        LOG.info ("Removing duplicate entries..");
        Dataset<Row> dupRemoved = orderPayments
                .dropDuplicates("order_id", "payment_sequential");

        return dupRemoved
                .groupBy ("order_id", "payment_sequential", "payment_value")
                .count().alias ("Total Count");
    }

    // Handle null data in the product dimensions
    public Dataset <Row> HandleNullData (Dataset<Row> products) {
        assert products != null;

        LOG.info ("Checking for the null entries in the product_weight");
        products
                .filter (col ("product_weight_g").isNull())
                .show (false);

        LOG.info ("Checking for the null entries in the product_length_cm");
        products
                .filter (col ("product_length_cm").isNull())
                .show (false);

        return products
                .withColumn ("product_weight_g", coalesce(col ("product_weight_g"), lit (100)))
                .withColumn ("product_length_cm", coalesce(col ("product_length_cm"), lit (20)));
    }
}
