from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("EcommerceRealTimeProcessing") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/ecommerce") \
    .getOrCreate()

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

inventory_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

notification_schema = StructType([
    StructField("notification_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("message", StringType(), True),
    StructField("timestamp", StringType(), True)
])

def process_topic(topic, schema, collection):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .load()

    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    cleaned_df = df.dropna()

    query = cleaned_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: batch_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "ecommerce") \
            .option("collection", collection) \
            .save()) \
        .outputMode("append") \
        .start()

    return query

order_query = process_topic("orders", order_schema, "orders")
inventory_query = process_topic("inventory", inventory_schema, "inventory")
notification_query = process_topic("notifications", notification_schema, "notifications")

order_query.awaitTermination()
inventory_query.awaitTermination()
notification_query.awaitTermination()