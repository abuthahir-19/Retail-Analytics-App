package com.learnJava.lib;

import com.learnJava.driver.SparkUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;


public class TransformationTest {
    private SparkSession spark;
    private static final Logger LOG = LogManager.getLogger();

    @BeforeEach
    public void setUp () {
        spark = SparkUtil.getSparkSession("Spark Testing Session Establishment");
    }

    @AfterEach
    public void stopSetup () {
        spark.close();
    }

    @Test
    public void TestFilterOrders() {
        StructType ordersSchema = new StructType()
                .add ("order_id", DataTypes.StringType, false)
                .add ("customer_id", DataTypes.StringType, false)
                .add ("order_status", DataTypes.StringType, false)
                .add ("order_purchase_timestamp", DataTypes.StringType, false)
                .add ("order_approved_at", DataTypes.StringType, false)
                .add ("order_delivered_carrier_date", DataTypes.StringType, false)
                .add ("order_delivered_customer_date", DataTypes.StringType, false)
                .add ("order_estimated_delivery_date", DataTypes.StringType, false);

        // Create the test data
        List <Row> data = Arrays.asList(
            RowFactory.create ("e481f51cbdc54678b7cc49136f2d6af7","9ef432eb6251297304e76186b10a928d","shipped","2017-10-02 10:56:33","2017-10-02 11:07:15","2017-10-04 19:55:00","2017-10-10 21:25:13","2017-10-18 00:00:00"),
            RowFactory.create ("53cdb2fc8bc7dce0b6741e2150273451","b0830fb4747a6c6d20dea0b8c802d7ef","cancelled","2018-07-24 20:41:37","2018-07-26 03:24:27","2018-07-26 14:31:00","2018-08-07 15:27:45","2018-08-13 00:00:00"),
            RowFactory.create ("47770eb9100c2d0c44946d9cf07ec65d","41ce2a54c0b03bf3443c3d931a367089","invoiced","2018-08-08 08:38:49","2018-08-08 08:55:23","2018-08-08 13:50:00","2018-08-17 18:06:29","2018-09-04 00:00:00"),
            RowFactory.create ("949d5b44dbf5de918fe9c16f97b45f8a","f88197465ea7920adcdbec7375364d82","created","2017-11-18 19:28:06","2017-11-18 19:45:59","2017-11-22 13:39:59","2017-12-02 00:28:42","2017-12-15 00:00:00"),
            RowFactory.create ("ad21c59c0840e6cb83a9ceb5573f8159","8ab97904e6daea8866dbdbc4fb7aad2c","delivered","2018-02-13 21:18:39","2018-02-13 22:20:29","2018-02-14 19:46:34","2018-02-16 18:17:02","2018-02-26 00:00:00"),
            RowFactory.create ("a4591c265e18cb1dcee52889e2d8acc3","503740e9ca751ccdda7ba28e9ab8f608","unavailable","2017-07-09 21:57:05","2017-07-09 22:10:13","2017-07-11 14:58:04","2017-07-26 10:57:55","2017-08-01 00:00:00"),
            RowFactory.create ("6514b8ad8028c9f2cc2374ded245783f","9bdf08b4b3b52b5526ff42d37d47f222","processing","2017-05-16 13:10:30","2017-05-16 13:22:11","2017-05-22 10:07:46","2017-05-26 12:55:51","2017-06-07 00:00:00"),
            RowFactory.create ("76c6e866289321a7c93b82b54852dc33","f54a9f0e6b351c431402b8461ea51999","approved","2017-01-23 18:29:09","2017-01-25 02:50:47","2017-01-26 14:16:31","2017-02-02 14:08:10","2017-03-06 00:00:00")
        );

        Dataset <Row> orders = spark.createDataFrame(data, ordersSchema);

        LOG.info ("Sample test data");
        orders.show(false);
        // Do run filter logic
        Dataset<Row> filteredOrders = orders.select ("order_status")
                .filter (col ("order_status").isInCollection(Arrays.asList ("shipped", "invoiced", "created", "delivered", "processing", "approved")));

        // Check the filtered Orders order status
        List<Row> order_statuses = filteredOrders.select (col ("order_status"))
                .distinct()
                .collectAsList();

        int c = (int) order_statuses.stream()
                .map (Row::toString)
                .filter (status -> status.equalsIgnoreCase("cancelled") || status.equalsIgnoreCase("unavailable"))
                .count();

        assertEquals(0, c, "Count matches");
    }
}
