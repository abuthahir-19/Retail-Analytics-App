package com.learnJava.driver;

import org.apache.spark.sql.SparkSession;

public class SparkUtil {
    public static SparkSession getSparkSession (String appName) {
        return SparkSession
            .builder()
            .appName(appName)
            .master("local[*]")
            .getOrCreate();
    }
}
