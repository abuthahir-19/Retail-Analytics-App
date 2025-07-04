package com.learnJava.datasink;

import com.learnJava.driver.SparkUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ClickHouseDemo {
    private static final Logger LOG = LogManager.getLogger();

    public static void main(String[] args) throws NoSuchTableException {
        Properties props = new Properties();
        try (InputStream in = ClickHouseDemo.class.getClassLoader().getResourceAsStream("clickhouse_config.properties")) {
            props.load (in);
        } catch (IOException e) {
            LOG.error ("Exception while reading the file : {}", e.getMessage());
        }

        LOG.info ("Properties loaded : {}", props);

        SparkSession spark = SparkUtil.getSparkSession("ClickHouse Demo Application", props);

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
        });


        List<Row> data = Arrays.asList(
                RowFactory.create(1, "Alice"),
                RowFactory.create(2, "Bob"),
                RowFactory.create(3, "John"),
                RowFactory.create(4, "David"),
                RowFactory.create (5, "Ajay")
        );

        // Create a DataFrame
        Dataset<Row> df = spark.createDataFrame(data, schema);

        df
        .writeTo("clickhouse.default.sample_test")
        .append();

        spark.stop();
    }
}
