package com.learnJava.driver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class StreamDataToKafka {
    private static final Logger LOG = LogManager.getLogger();
    public static void publishToKafka (Dataset <Row> data, String topicName) {
        LOG.info ("Writing the data to kafka topic {}", topicName);
        try {
            data.selectExpr("to_json(struct(*)) as value")
                .write()
                .format ("kafka")
                .option ("kafka.bootstrap.servers", "localhost:9092")
                .option ("topic", topicName)
                .save();
            LOG.info ("Data written to kafka topic {} successfully.", topicName);
        } catch (Exception e) {
            LOG.info ("Exception while writing data to kafka\nError details : {}", e.getMessage());
        }
    }
}
