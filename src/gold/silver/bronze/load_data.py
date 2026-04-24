from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.range(100)
df.write.mode("overwrite").saveAsTable("bronze_table")