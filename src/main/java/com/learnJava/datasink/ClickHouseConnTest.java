package com.learnJava.datasink;

import com.learnJava.lib.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ClickHouseConnTest {
    public static void main(String[] args) {
        String jdbcUrl = "jdbc:clickhouse://d2ias78hi7.us-east1.gcp.clickhouse.cloud:8443/default";
        Properties props = new Properties();
        props.put("user", "default");
        props.put("password", Constants.ch_password); // Replace with your actual password
        props.put("ssl", "true");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            System.out.println("Connection successful!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
