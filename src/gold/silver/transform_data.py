from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.table("bronze_table")
df_filtered = df.filter("id > 10")

df_filtered.write.mode("overwrite").saveAsTable("silver_table")