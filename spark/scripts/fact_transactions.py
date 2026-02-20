from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr

spark = SparkSession.builder \
    .appName("fact_transaction_pipeline") \
    .getOrCreate()


# READ SOURCE DATA (CSV)
orders = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/orders.csv")

order_items = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data/order_items.csv")

# DATA CLEANING
# Filter invalid quantity & price
order_items_clean = order_items.filter(
    (col("quantity") > 0) & (col("price") > 0)
)

# Filter invalid order_id
orders_clean = orders.filter(col("order_id").isNotNull())


# DATA STANDARDIZATION
# Cast numeric columns
order_items_clean = order_items_clean \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("price", col("price").cast("double"))

orders_clean = orders_clean \
    .withColumn("total_amount", col("total_amount").cast("double"))

# Standardize date format
orders_clean = orders_clean.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)

# JOIN DATASETS
fact_df = orders_clean.join(
    order_items_clean,
    on="order_id",
    how="inner"
)

# DERIVED COLUMN
# Create GMV (Gross Merchandise Value) per transaction line
fact_df = fact_df.withColumn(
    "gmv",
    expr("quantity * price")
)

# WRITE OUTPUT (PARQUET)
fact_df.write \
    .mode("overwrite") \
    .parquet("data/fact_transactions")

# VALIDATION
fact_check = spark.read.parquet("data/fact_transactions")
fact_check.show(5)
fact_check.printSchema()

spark.stop()