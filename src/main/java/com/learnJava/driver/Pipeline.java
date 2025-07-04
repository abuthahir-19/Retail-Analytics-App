package com.learnJava.driver;

import com.learnJava.lib.Transformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import static org.apache.spark.sql.functions.*;

import static com.learnJava.lib.Constants.*;
import static com.learnJava.lib.DataSchemaDefinition.*;

public class Pipeline {
    private SparkSession spark = null;
    private static final Logger LOG = LogManager.getLogger();

    public Pipeline (SparkSession spark) {
        this.spark = spark;
    }

    public void BeginTransformation () throws NoSuchTableException {
        LOG.info ("Initiating the transformation..");
        Transformation trans = new Transformation(spark);

        Dataset<Row> orders = ConsumeDataFromKafka.ReadDataFromKafka(spark, orders_topic, ordersSchema);
        Dataset<Row> orderItems = ConsumeDataFromKafka.ReadDataFromKafka(spark, order_items_topic, orderItemsSchema);
        Dataset<Row> customers = ConsumeDataFromKafka.ReadDataFromKafka(spark, customers_topic, customerSchema);
        Dataset<Row> sellers = ConsumeDataFromKafka.ReadDataFromKafka(spark, sellers_topic, sellerSchema);
        Dataset<Row> orderPayments = ConsumeDataFromKafka.ReadDataFromKafka(spark, order_payments_topic, orderPaymentsSchema);
        Dataset<Row> products = ConsumeDataFromKafka.ReadDataFromKafka(spark, products_topic, productsSchema);

        // Rule 1: Filter out the order which is of either cancelled or unavailable
        Dataset<Row> filteredOrders = trans.FilterOrders(orders);
        assert filteredOrders != null;
        LOG.info ("Writing the filtered output to clickhouse");

        filteredOrders
                .writeTo("clickhouse.default.filtered_orders")
                .append();

        LOG.info ("Checking the count of order in each order status");
        filteredOrders.groupBy (col ("order_status"))
                .count().alias ("Total Count")
                .show(false);

        // Rule 2: Do Data enrichment by adding customer and seller localities information to the orders items dataset
        assert orderItems != null;
        Dataset <Row> enriched_order_items = trans.DoDataEnrichment(
                orderItems,
                orders,
                customers,
                sellers
        );

        LOG.info ("Order Items data after adding customer and seller localities information");
        LOG.info ("Writing the enriched order items data to clickhouse");
        enriched_order_items
                .writeTo ("clickhouse.default.enriched_orders_items")
                .append();

        // Rule 3: Identify the high value customer
        Dataset <Row> CustomerScore = trans.GetCustomerScore(orderItems, orders);
        LOG.info ("Top 10 Customers with high order value");
        LOG.info ("Writing the customer score data to clickhouse");
        CustomerScore
                .writeTo ("clickhouse.default.customer_score_data")
                .append();

        // Rule 4: Identify the Logistics performance by finding the delay in delivery
        assert orders != null;
        Dataset <Row> OrderDelays = trans.CalculateDeliveryDelay(orders);

        LOG.info ("Added a feature that will show the number of days delay in the delivery");
        LOG.info ("Writing the delivery stat to clickhouse");
        OrderDelays
                .writeTo("clickhouse.default.delivery_stat")
                .append();

        // Rule 5: Predict High value transactions to find fraudulent transactions
        assert orderPayments != null;
        Dataset <Row> highValueTransactions = trans.FlagHighValueOrders(orderPayments);

        LOG.info ("Getting the information of top 10 high value transaction in each payment method (Potentially Fraud)");
        LOG.info ("Writing the top 10 high value transactions in each payment type to clickhouse");
        highValueTransactions
                .writeTo("clickhouse.default.high_value_order_stat")
                .append();

        // Rule 6: Identify top-selling product category
        Dataset <Row> topSellingProduct = trans.PredictTopSellingProductCategory(orderItems, products, orders);

        LOG.info ("Getting the top selling product category in an hourly window");
        LOG.info ("Writing the top selling product category information to clickhouse");
        topSellingProduct
                .writeTo ("clickhouse.default.top_selling_product_category")
                .append();

        // Rule 7: Apply the discount to the credit card purchase
        Dataset<Row> discountedPrice = trans.ApplyDiscountsForCreditCard(orderPayments, orderItems);

        LOG.info ("Applying the discount price for credit card purchase");
        LOG.info ("Writing the credit card discounts data to clickhouse");
        discountedPrice
                .writeTo ("clickhouse.default.credit_card_dscns")
                .append();

        // Rule 8: Filtering the duplicate data in order payments to avoid invalid revenue report
        Dataset <Row> deduped = trans.DataDeduplication(orderPayments);
        LOG.info ("Checking the entries after removing the duplicate");
        LOG.info ("Writing the deduped order_payments info to clickhouse");
        deduped
                .writeTo("clickhouse.default.order_pymnts_dedup")
                .append();

        // Rule 9: Handling the null values in the product dimensions
        assert products != null;
        Dataset<Row> NullRemoved = trans.HandleNullData(products);
        LOG.info ("Filling null entries in the products dataset with random values");
        LOG.info ("Writing the products information to clickhouse after removing null entries in the product dimension");
        NullRemoved
                .writeTo("clickhouse.default.products_cleaned")
                .append();

        LOG.info ("Getting null count in the product dimension(product_weight_g, product_length_cm) after removing null");
        System.out.println ("Null Count in products : " + NullRemoved.count());
    }
}
