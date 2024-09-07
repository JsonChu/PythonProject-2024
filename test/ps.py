from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("YourAppName").getOrCreate()
data = spark.read.format("csv").option("header", "true").load("/test/a.csv")
data.show()
print(data.count())
data.write.format("csv").option("header", "true").save("/test/result")



