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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Main {
    private static final Logger LOG = LogManager.getLogger();
    public static void main(String[] args) throws Exception {
        if (args.length != 0) {
            LOG.error ("Usage : <hdfs_file_path>");
            System.exit(1);
        }
        Properties props = new Properties();

        try (InputStream in = new FileInputStream(args[0])) {
            LOG.info ("Loading in try");
            props.load (in);
            LOG.info ("Properties loaded !!");
        } catch (FileNotFoundException e) {
            LOG.info ("Loading from HDFS");
            FileSystem fs = FileSystem.get (HadoopConfigUtil.getConfiguration());
            InputStream in = fs.open (new Path (args[0]));
            props.load (in);
            LOG.info ("Properties loaded : {}", props);
        } catch (Exception e) {
            LOG.error ("Error while loading the properties file : {}", e.getMessage());
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