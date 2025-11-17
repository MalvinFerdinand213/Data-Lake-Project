from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import os

spark = SparkSession.builder.appName("SilverTransform").getOrCreate()

# Paths
bronze_orders = "C:/spark/data_lake/bronze/orders/"
bronze_users = "C:/spark/data_lake/bronze/users/"

silver_orders = "C:/spark/data_lake/silver/orders/"
silver_users = "C:/spark/data_lake/silver/users/"

# Load Bronze
orders = spark.read.parquet(bronze_orders)
users = spark.read.parquet(bronze_users)

print(f"[Silver] Loaded Bronze Orders: {orders.count()}, Users: {users.count()}")

# Clean orders
orders_clean = orders.withColumn("amount", col("amount").cast("double")) \
                     .withColumn("timestamp", to_timestamp("timestamp"))

# Drop rows with nulls in critical columns
orders_clean = orders_clean.dropna(subset=["user_id", "amount", "timestamp"])

print(f"[Silver] Cleaned Orders count: {orders_clean.count()}")

# Pastikan folder Silver ada
os.makedirs(silver_orders, exist_ok=True)
os.makedirs(silver_users, exist_ok=True)

# Write Silver
orders_clean.write.mode("overwrite").parquet(silver_orders)
users.write.mode("overwrite").parquet(silver_users)

print("[Silver] Transformation completed.")
