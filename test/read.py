

from pyspark.sql import SparkSession
# 创建SparkSession，并启用Hive支持

spark = SparkSession.builder \
    .appName("HiveExample") \
    .master("yarn") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()



# 读取Hive表的数据
input_table = "default.info"
df = spark.sql(f"SELECT id, age ,name FROM {input_table}")

# 将年龄加5
df_with_age_incremented = df.withColumn("age", df["age"] + 5)

# 写入到另一张表
output_table = "default.result"
df_with_age_incremented.write.mode("overwrite").saveAsTable(output_table)

# 停止SparkSession
spark.stop()