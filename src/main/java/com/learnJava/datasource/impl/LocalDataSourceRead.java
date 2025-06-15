package com.learnJava.datasource.impl;

import com.learnJava.datasource.DataSource;
import com.learnJava.lib.ReadDataset;
import com.learnJava.model.Datasets;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LocalDataSourceRead implements DataSource {
    @Override
    public Dataset<Row> readDataset (SparkSession spark, Datasets data) {
        return ReadDataset.getDataset(spark, data.path, data.format);
    }
}
