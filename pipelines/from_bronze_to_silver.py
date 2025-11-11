# Databricks notebook source
catalog = "workspace"
schema = "raw"
volume = "tables"

# COMMAND ----------

csv_rel_paths = ['olist_customers_dataset.csv',
'olist_geolocation_dataset.csv',
'olist_order_items_dataset.csv',
'olist_order_payments_dataset.csv',
'olist_order_reviews_dataset.csv',
'olist_orders_dataset.csv',
'olist_products_dataset.csv',
'olist_sellers_dataset.csv',
]

table_names = ['customers',
'geolocation',
'order_items',
'order_payments',
'order_reviews',
'orders',
'products',
'sellers',
]

# COMMAND ----------

spark.sql(f"use catalog {catalog}")
spark.sql(f"use schema {schema}")

# COMMAND ----------

for csv_rel_path, table_name in zip(csv_rel_paths, table_names):
    csv_path = f"/Volumes/{catalog}/{schema}/{volume}/{csv_rel_path}"

    df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(csv_path)
    )

    (df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.{table_name}")
    )
