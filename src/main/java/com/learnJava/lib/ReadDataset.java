package com.learnJava.lib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadDataset {
    public static Dataset<Row> getDataset (SparkSession spark, String path, String format) {
        return spark
            .read()
            .format (format)
            .option ("header", "true")
            .option ("inferSchema", "true")
            .load (path);
    }
}
