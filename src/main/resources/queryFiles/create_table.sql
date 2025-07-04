CREATE TABLE default.products_de_null (
   product_id String,
   product_category_name String,
   product_name_length Int,
   product_description_length Int,
   product_photos_qty Int,
   product_weight_g Int,
   product_length_cm Int,
   product_height_cm Int,
   product_width_cm Int
) ENGINE = MergeTree()
ORDER BY product_id;
