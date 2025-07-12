package com.learnJava.driver;

import com.learnJava.model.Datasets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.learnJava.lib.Constants.*;

public class DatasetsList {
    private static final Logger LOG = LogManager.getLogger();

    public static Properties props;
    public DatasetsList (Properties propsObj) {
        props = propsObj;
        LOG.info ("Properties assigned : {}", props);
    }

    public List<Datasets> getDatasetsList () {
        Datasets customer = new Datasets (customers_topic, props.getProperty(CUSTOMERS), "csv");
        Datasets orderItems = new Datasets (order_items_topic, props.getProperty (ORDER_ITEMS), "csv");
        Datasets orderPayments = new Datasets (order_payments_topic, props.getProperty(ORDER_PAYMENTS), "csv");
        Datasets orders = new Datasets (orders_topic, props.getProperty(ORDERS), "csv");
        Datasets products = new Datasets (products_topic, props.getProperty (PRODUCTS), "csv");
        Datasets sellers = new Datasets (sellers_topic, props.getProperty(SELLERS), "csv");

        LOG.info ("checking sample props Customers : {}", props.getProperty(CUSTOMERS));
        return Arrays.asList (customer, orderItems, orderPayments, orders, products, sellers);
    }
}
