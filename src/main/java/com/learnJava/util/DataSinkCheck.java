package com.learnJava.util;

import com.learnJava.driver.ConsumeDataFromKafka;
import com.learnJava.driver.SparkUtil;
import com.learnJava.lib.Transformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.learnJava.lib.Constants.*;
import static com.learnJava.lib.DataSchemaDefinition.*;

public class DataSinkCheck {
    private static final Logger LOG = LogManager.getLogger();
    public static void main(String[] args) throws NoSuchTableException, IOException {
        InputStream in = DataSinkCheck.class.getClassLoader().getResourceAsStream("clickhouse_config.properties");
        Properties props = new Properties();
        props.load (in);

        SparkSession spark = SparkUtil.getSparkSession("Clickhouse Datasink Demo", props);

        Dataset<Row> products = ConsumeDataFromKafka.ReadDataFromKafka(spark, products_topic, productsSchema);

        Transformation trans = new Transformation(spark);
        Dataset<Row> deduped = trans.HandleNullData(products);

        LOG.info ("Total number of records : {}", deduped.count());

        LOG.info ("writing the data to clickhouse");
        deduped
                .writeTo("clickhouse.default.products_cleaned")
                .append();

        LOG.info ("Closing the spark session");
        spark.close();
    }
}

//99441