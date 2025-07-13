package com.learnJava.util;

import com.learnJava.driver.DatasetsList;
import com.learnJava.driver.SparkUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DataSinkCheck {
    private static final Logger LOG = LogManager.getLogger();
    public static void main(String[] args) throws NoSuchTableException, IOException {
        if (args.length != 1) {
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
            InputStream in = fs.open (new Path(args[0]));
            props.load (in);
            LOG.info ("Properties loaded : {}", props);
        } catch (Exception e) {
            LOG.error ("Error while loading the properties file : {}", e.getMessage());
        }

        LOG.info ("Initiating the spark session");
        SparkSession spark = SparkUtil.getSparkSession("Data sink check", props);

        DatasetsList datasetsList = new DatasetsList(props);

    }
}

//99441