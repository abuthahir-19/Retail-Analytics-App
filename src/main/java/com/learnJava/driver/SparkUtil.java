package com.learnJava.driver;

import com.learnJava.lib.Constants;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SparkUtil {
    public static SparkSession getSparkSession (String appName, Properties props) {
        Map<String, Object> configMap = new HashMap<>();
        for (String prop : props.stringPropertyNames()) {
            configMap.put (prop, props.getProperty(prop));
        }

        return SparkSession
            .builder()
            .appName(appName)
            .master("local[*]")
            .config(configMap)
            .getOrCreate();
    }

    public static SparkSession getSparkSession (String appName) {
        return SparkSession
                .builder()
                .appName(appName)
                .master("local[*]")
                .getOrCreate();
    }
}
