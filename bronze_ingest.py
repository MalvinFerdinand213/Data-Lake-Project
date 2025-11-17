from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("BronzeIngest").getOrCreate()

# Paths
orders_csv = r"C:/Kawan Lama/Project 4/data_lake_project/data/orders.csv"
users_json  = r"C:/Kawan Lama/Project 4/data_lake_project/data/users.json"

bronze_orders = "C:/spark/data_lake/bronze/orders/"
bronze_users = "C:/spark/data_lake/bronze/users/"

# Load data
orders = spark.read.csv(orders_csv, header=True, inferSchema=True)
users = spark.read.json(users_json)

# Pastikan folder Bronze ada
os.makedirs(bronze_orders, exist_ok=True)
os.makedirs(bronze_users, exist_ok=True)

# Write Bronze Parquet
orders.write.mode("overwrite").parquet(bronze_orders)
users.write.mode("overwrite").parquet(bronze_users)

print(f"[Bronze] Orders count: {orders.count()}, Users count: {users.count()}")
print("[Bronze] Ingest completed.")
