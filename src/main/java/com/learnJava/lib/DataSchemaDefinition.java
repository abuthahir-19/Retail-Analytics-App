package com.learnJava.lib;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DataSchemaDefinition {
    public static final StructType customerSchema =new StructType()
            .add ("customer_id", DataTypes.StringType, false)
            .add ("customer_unique_id", DataTypes.StringType, false)
            .add ("customer_zip_code_prefix", DataTypes.IntegerType, false)
            .add ("customer_city", DataTypes.StringType, false)
            .add ("customer_state", DataTypes.StringType, false);

    public static final StructType orderItemsSchema = new StructType()
            .add ("order_id", DataTypes.StringType, false)
            .add ("order_item_id", DataTypes.IntegerType, false)
            .add ("product_id", DataTypes.StringType, false)
            .add ("seller_id", DataTypes.StringType, false)
            .add ("shipping_limit_date", DataTypes.TimestampType, false)
            .add ("price", DataTypes.DoubleType, false)
            .add ("freight_value", DataTypes.DoubleType, false);

    public static final StructType orderPaymentsSchema = new StructType()
            .add ("order_id", DataTypes.StringType, false)
            .add ("payment_sequential", DataTypes.IntegerType, false)
            .add ("payment_type", DataTypes.StringType, false)
            .add ("payment_installments", DataTypes.IntegerType, false)
            .add ("payment_value", DataTypes.DoubleType, false);

    public static final StructType productsSchema = new StructType()
            .add ("product_id", DataTypes.StringType, true)
            .add ("product_category_name", DataTypes.StringType, true)
            .add ("product_name_length", DataTypes.IntegerType, true)
            .add ("product_description_length", DataTypes.IntegerType, true)
            .add ("product_photos_qty", DataTypes.IntegerType, true)
            .add ("product_weight_g", DataTypes.IntegerType, true)
            .add ("product_length_cm", DataTypes.IntegerType, true)
            .add ("product_height_cm", DataTypes.IntegerType, true)
            .add ("product_width_cm", DataTypes.IntegerType, true);

    public static final StructType ordersSchema = new StructType()
            .add ("order_id", DataTypes.StringType, false)
            .add ("customer_id", DataTypes.StringType, false)
            .add ("order_status", DataTypes.StringType, false)
            .add ("order_purchase_timestamp", DataTypes.TimestampType, false)
            .add ("order_approved_at", DataTypes.TimestampType, false)
            .add ("order_delivered_carrier_date", DataTypes.TimestampType, false)
            .add ("order_delivered_customer_date", DataTypes.TimestampType, false)
            .add ("order_estimated_delivery_date", DataTypes.TimestampType, false);

    public static final StructType sellerSchema = new StructType()
            .add ("seller_id", DataTypes.StringType, false)
            .add ("seller_zip_code", DataTypes.IntegerType, false)
            .add ("seller_city", DataTypes.StringType, false)
            .add ("seller_state", DataTypes.StringType, false);
}
