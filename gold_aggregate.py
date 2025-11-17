import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count

spark = SparkSession.builder.appName("GoldAggregate").getOrCreate()

# Paths
silver_orders = "C:/spark/data_lake/silver/orders/"
silver_users = "C:/spark/data_lake/silver/users/"
gold_path = "C:/spark/data_lake/gold/sales_by_country/"

# Cek Silver sebelum baca
if not os.path.exists(silver_orders) or len(os.listdir(silver_orders)) == 0:
    raise FileNotFoundError("Silver orders folder tidak ada atau kosong. Jalankan Silver transform dulu!")

# Load Silver
orders = spark.read.parquet(silver_orders)
users = spark.read.parquet(silver_users)

print(f"[Gold] Loaded Silver Orders: {orders.count()}, Users: {users.count()}")

# Pastikan user_id sama tipe
orders = orders.withColumn("user_id", col("user_id").cast("string"))
users = users.withColumn("user_id", col("user_id").cast("string"))

# Join
joined = orders.join(users, on="user_id", how="inner")
print(f"[Gold] Joined count: {joined.count()}")

# Aggregate
country_sales = joined.groupBy("country") \
                      .agg(
                          _sum("amount").alias("total_sales"),
                          count("order_id").alias("total_orders")
                      )

print(f"[Gold] Aggregated countries: {country_sales.count()}")

# Pastikan folder Gold ada
os.makedirs(gold_path, exist_ok=True)

# Write Gold
country_sales.write.mode("overwrite").parquet(gold_path)
print(f"[Gold] Written to {gold_path}")
print("Pipeline Bronze → Silver → Gold completed successfully!")
