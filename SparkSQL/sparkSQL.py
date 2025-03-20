from pyspark.sql import SparkSession
import random

spark = SparkSession.builder \
    .appName("SparkSQL") \
    .master("local[*]") \
    .config("spark_executor_memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext
# 1,123 , 2,340958, 3,239847 ...
rdd = sc.parallelize(range(1,11)) \
    .map(lambda x: (x, random.randint(0,99) * x))
print(rdd.collect())


df = spark.createDataFrame(rdd, ["id", "value"])

print(df.show())

