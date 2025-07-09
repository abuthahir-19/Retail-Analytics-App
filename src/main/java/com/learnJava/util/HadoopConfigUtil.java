package com.learnJava.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;

public class HadoopConfigUtil {
    private static final Logger LOG = LogManager.getLogger();
    public static Configuration getConfiguration () {
        LOG.info ("Initializing the configuration..");
        Configuration conf = new Configuration();
        InputStream configFile = HadoopConfigUtil.class.getClassLoader().getResourceAsStream("core-site.xml");

        if (configFile == null) {
            LOG.info ("core-site.xml file does not exists !!");
        } else {
            LOG.info ("config file is present and trying to load it !!");
            conf.addResource(configFile);
            LOG.info ("config file loaded to classpath");
            LOG.info ("Loaded fs.defaultFS: {}", conf.get ("fs.defaultFS"));
            conf.set ("fs.defaultFS", "hdfs://192.168.1.37:9000");
            LOG.info ("Forced fs.defaultFS: {}", "hdfs://192.168.1.37:9000");
        }
        return conf;
    }
}
