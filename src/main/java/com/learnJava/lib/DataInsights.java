package com.learnJava.lib;

import com.learnJava.util.HadoopConfigUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Properties;

public class DataInsights {
    private static final Logger LOG = LogManager.getLogger();
    public static void main(String[] args) throws IOException {
        String hdfsPath = "hdfs://192.168.1.37:9000/user/abuthahir/retail_project/static_resource/clickhouse_config.properties";

        FileSystem fs = FileSystem.get (HadoopConfigUtil.getConfiguration());
        Properties props = new Properties();

        try (InputStream in = fs.open (new Path(hdfsPath))) {
            LOG.info ("Trying to load properties");
            props.load (in);
            LOG.info ("Properties loaded : {}", props);
        } catch (Exception e) {
            LOG.error ("Error while loading the properties !!");
            LOG.info ("Error details : {}", e.getMessage());
        }
    }
}
