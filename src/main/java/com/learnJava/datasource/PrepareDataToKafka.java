package com.learnJava.datasource;

import com.learnJava.datasource.impl.LocalDataSourceRead;
import com.learnJava.model.Datasets;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrepareDataToKafka {
    private SparkSession spark;
    private Map<Dataset<Row>, String> dataMap = new HashMap<>();

    public PrepareDataToKafka (SparkSession spark) {
        this.spark = spark;
    }

    public void setDataframesList (List<Datasets> datasets) {
        LocalDataSourceRead sourceRead = new LocalDataSourceRead();
        for (Datasets dataset : datasets) {
            dataMap.put (sourceRead.readDataset(spark, dataset), dataset.name);
        }
    }

    public Map<Dataset<Row>, String> getDataframesList () {
        return this.dataMap;
    }
}
