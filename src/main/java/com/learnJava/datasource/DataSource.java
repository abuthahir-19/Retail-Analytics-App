package com.learnJava.datasource;

import com.learnJava.model.Datasets;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface DataSource {
    Dataset<Row> readDataset (SparkSession spark, Datasets dataset);
}
