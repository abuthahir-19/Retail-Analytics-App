package com.learnJava.driver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class ConsumeDataFromKafka {
    private static final Logger LOG = LogManager.getLogger();
    public static Dataset <Row> ReadDataFromKafka (SparkSession spark, String topicName, StructType schema) {
        LOG.info ("Reading the data from the specified kafka topic {}", topicName);
        try {
            Dataset<Row> kafka_df = spark
                    .read()
                    .format("kafka")
                    .option ("kafka.bootstrap.servers", "localhost:9092")
                    .option ("subscribe", topicName)
                    .option ("startingOffset", "earliest")
                    .load();

            LOG.info ("Data was read from the kafka topic successfully !!");
            Dataset <Row> decoded_data_df = kafka_df.select (
                 from_json(
                         col("value").cast ("string"),
                         schema
                 ).alias ("data")
            )
            .select ("data.*");

            LOG.info ("Displaying the sample data to the console from the topic {}.", topicName);
            return decoded_data_df;
        } catch (Exception e) {
            LOG.info ("Error while reading the data from kafka topic {}", topicName);
            LOG.info ("Error details : {}", e.getMessage());
        }
        return null;
    }
}
