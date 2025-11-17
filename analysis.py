from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder.appName("GoldAnalysis").getOrCreate()

# Load data Gold
gold_orders = spark.read.parquet("C:/spark/data_lake/gold/sales_by_country/")

# Verifikasi data
gold_orders.show()
gold_orders.printSchema()

# Contoh analisis: total amount per user
gold_orders.groupBy("user_id").agg(sum("amount").alias("total_spent")).show()

# Stop Spark session
spark.stop()
