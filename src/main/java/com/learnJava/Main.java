package com.learnJava;

import com.learnJava.datasource.PrepareDataToKafka;
import com.learnJava.driver.DatasetsList;
import com.learnJava.driver.Pipeline;
import com.learnJava.driver.SparkUtil;
import com.learnJava.driver.StreamDataToKafka;
import com.learnJava.model.Datasets;
import com.learnJava.util.HadoopConfigUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Main {
    private static final Logger LOG = LogManager.getLogger();
    public static void main(String[] args) throws Exception {
        String filePath = "hdfs://192.168.1.37:9000/user/abuthahir/retail_analytics_proj/static_resource/clickhouse_config.properties";
        Properties props = new Properties();
        Path configPath = new Path (filePath);

        FileSystem fs = FileSystem.get (HadoopConfigUtil.getConfiguration());
        try (InputStream in = fs.open(configPath)) {
            props.load (in);
            LOG.info ("Properties loaded successfully from the path : {}", filePath);
        } catch (Exception e) {
            LOG.error ("Error while loading the properties : {}", e.getMessage());
        }

        LOG.info ("Initiating the spark session");
        SparkSession spark = SparkUtil.getSparkSession("Real-Time Retail Data Analytics", props);

        LOG.info ("Preparing the data from local to kafka");
        List<Datasets> datasets = new DatasetsList().getDatasetsList();

        PrepareDataToKafka prepareDataToKafka = new PrepareDataToKafka(spark);
        prepareDataToKafka.setDataframesList(datasets);

        Map<Dataset<Row>, String> dataMap = prepareDataToKafka.getDataframesList();

        LOG.info ("Stream the data into kafka");
        for (Map.Entry<Dataset<Row>, String> entry : dataMap.entrySet()) {
            StreamDataToKafka.publishToKafka(entry.getKey(), entry.getValue());
        }
        LOG.info ("Data streamed to kafka !!");

        LOG.info ("Invoking the data transformation pipeline..");
        Pipeline pipeline = new Pipeline(spark);
        pipeline.BeginTransformation();


        LOG.info ("Closing the spark session");
        spark.close();
    }
}