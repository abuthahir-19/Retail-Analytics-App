package com.learnJava;

import com.learnJava.driver.SparkUtil;
import com.learnJava.driver.StreamDataToKafka;
import com.learnJava.lib.Constants;
import com.learnJava.lib.ReadDataset;
import com.learnJava.lib.Transformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Main {
    private static final Logger LOG = LogManager.getLogger();
    public static void main(String[] args) {
        LOG.info ("Initiating the spark session");
        SparkSession spark = SparkUtil.getSparkSession("Real-Time Retail Data Analytics");

        LOG.info ("Reading the data");
        Dataset <Row> customers_df = ReadDataset.getDataset(spark, Constants.customers_file_path, "csv");
        Dataset <Row> order_items_df = ReadDataset.getDataset(spark, Constants.order_items_file_path, "csv");
        Dataset <Row> order_payments_df = ReadDataset.getDataset(spark, Constants.order_payments_file_path, "csv");
        Dataset <Row> orders_df = ReadDataset.getDataset(spark, Constants.orders_file_path, "csv");
        Dataset <Row> products_df = ReadDataset.getDataset(spark, Constants.products_file_path, "csv");
        Dataset <Row> sellers_df = ReadDataset.getDataset(spark, Constants.sellers_file_path, "csv");
        LOG.info ("Data loaded successfully");


        LOG.info ("Stream the data into kafka");
        StreamDataToKafka.publishToKafka(customers_df, Constants.customers_topic);
        StreamDataToKafka.publishToKafka(order_items_df, Constants.order_items_topic);
        StreamDataToKafka.publishToKafka(order_payments_df, Constants.order_payments_topic);
        StreamDataToKafka.publishToKafka(orders_df, Constants.order_topic);
        StreamDataToKafka.publishToKafka(products_df, Constants.products_topic);
        StreamDataToKafka.publishToKafka(sellers_df, Constants.sellers_topic);
        LOG.info ("Data streamed to kafka !!");


        // Invoking the Transformation to perform series of transformation ops
        Transformation trans = new Transformation(spark);

        // Rule 1: Filter out the order which is of either cancelled or unavailable
        Dataset<Row> filteredOrders = trans.FilterOrders();
        assert filteredOrders != null;
        filteredOrders.groupBy (col ("order_status"))
                    .count().alias ("Total Count")
                    .show (false);
        LOG.info ("Grouping the data based on the order status after the filtration");


        // Rule 2: Do Data enrichment by adding customer and seller localities information to the orders items dataset
        Dataset <Row> enriched_order_items = trans.DoDataEnrichment();

        LOG.info ("Order Items data after adding customer and seller localities information");
        enriched_order_items.show (false);

        // Rule 3: Identify the high value customer
        Dataset <Row> CustomerScore = trans.GetCustomerScore();
        LOG.info ("Top 10 Customers with high order value");
        CustomerScore.show(false);

        // Rule 4: Identify the Logistics performance by finding the delay in delivery
        Dataset <Row> OrderDelays = trans.CalculateDeliveryDelay();

        LOG.info ("Added a feature that will show the number of days delay in the delivery");
        OrderDelays.show (false);

        // Rule 5: Predict High value transactions to find fraudulent transactions
        Dataset <Row> highValueTransactions = trans.FlagHighValueOrders();

        LOG.info ("Getting the information of top 10 high value transaction in each payment method (Potentially Fraud)");
        highValueTransactions.show (false);

        // Rule 6: Identify top-selling product category
        Dataset <Row> topSellingProduct = trans.PredictTopSellingProductCategory();

        LOG.info ("Getting the top selling product category in an hourly window");
        topSellingProduct.show (false);

        // Rule 7: Apply the discount to the credit card purchase
        Dataset<Row> discountedPrice = trans.ApplyDiscountsForCreditCard();

        LOG.info ("Applying the discount price for credit card purchase");
        discountedPrice.show (false);

        // Rule 8: Filtering the duplicate data in order payments to avoid invalid revenue report
        Dataset <Row> deduped = trans.DataDeduplication();
        LOG.info ("Checking the entries after removing the duplicate");
        deduped.show(false);

        // Rule 9: Handling the null values in the product dimensions
        Dataset<Row> NullRemoved = trans.HandleNullData();
        LOG.info ("Filling null entries in the products dataset with random values");
        NullRemoved.show (false);

        LOG.info ("Getting null count in the product dimension(product_weight_g, product_length_cm) after removing null");
        System.out.println ("Null Count in products : " + NullRemoved.count());

        LOG.info ("Closing the spark session");
        spark.close();
    }
}