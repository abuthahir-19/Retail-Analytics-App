package com.learnJava.driver;

import com.learnJava.lib.Constants;
import com.learnJava.model.Datasets;

import java.util.Arrays;
import java.util.List;
import static com.learnJava.lib.Constants.*;

public class DatasetsList {
    Datasets customer = new Datasets (customers_topic, Constants.customers_file_path, "csv");
    Datasets orderItems = new Datasets (order_items_topic, Constants.order_items_file_path, "csv");
    Datasets orderPayments = new Datasets (order_payments_topic, Constants.order_payments_file_path, "csv");
    Datasets orders = new Datasets (orders_topic, Constants.orders_file_path, "csv");
    Datasets products = new Datasets (products_topic, Constants.products_file_path, "csv");
    Datasets sellers = new Datasets (sellers_topic, Constants.sellers_file_path, "csv");

    public List<Datasets> getDatasetsList () {
        return Arrays.asList (customer, orderItems, orderPayments, orders, products, sellers);
    }
}
