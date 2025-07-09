package com.learnJava.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HadoopConnTest {
    private static final Logger LOG = LogManager.getLogger();
    public static void main(String[] args) throws IOException {
        LOG.info ("Initializing configuration");
        Configuration conf = new Configuration();
        File configFile = new File (HadoopConnTest.class.getClassLoader().getResource("core-site.xml").getFile());

        if (!configFile.exists()) {
            LOG.info ("config file does not exists !!");
        }
        conf.addResource(new Path (configFile.toURI().toString()));
        LOG.info ("core-site.xml added to the configuration");

        FileSystem fs = FileSystem.get (conf);
        Path path = new Path ("hdfs://192.168.1.37:9000/user/abuthahir/retail_project/static_resource/clickhouse_config.properties");
        Properties props = new Properties();

        try (InputStream in = fs.open (path)) {
            props.load (in);
            LOG.info ("Properties loaded : {}", props);
            System.out.println ("Properties : " + props);
        } catch (Exception e) {
            LOG.info ("Error loading properties : {}", e.getMessage());
        }
    }
}
