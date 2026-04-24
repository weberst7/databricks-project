from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.table("silver_table")
df.groupBy().count().write.mode("overwrite").saveAsTable("gold_table")