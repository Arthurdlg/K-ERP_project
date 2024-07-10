import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pymongo import MongoClient

mongo_host = "localhost"
mongo_port = "27017"
mongo_database = "eCommerce"

def write_to_mongodb(batch_df, batch_id, collection_name):
    def save_to_mongodb(iterator):
        client = MongoClient(mongo_host, mongo_port)
        db = client[mongo_database]
        collection = db[collection_name]
        for record in iterator:
            collection.insert_one(record.asDict())
        client.close()

    batch_df.foreachPartition(save_to_mongodb)

spark = SparkSession.builder \
    .appName("EcommerceRealTimeProcessing") \
    .getOrCreate()

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
])

def process_topic(topic, schema, collection_name):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    cleaned_df = df.dropna()

    query = cleaned_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_to_mongodb(batch_df, batch_id, collection_name)) \
        .outputMode("append") \
        .start()

    return query

# Traiter les différents topics
order_query = process_topic("orders", order_schema, "orders")
inventory_query = process_topic("inventory", inventory_schema, "inventory")
notification_query = process_topic("notifications", notification_schema, "notifications")

order_query.awaitTermination()
inventory_query.awaitTermination()
notification_query.awaitTermination()
