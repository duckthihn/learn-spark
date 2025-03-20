from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("SparkSQL") \
    .master("local[*]") \
    .config("spark_executor_memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext

rdd = sc.parallelize(
    [
        Row(id=1, name="quang linh", gender="Male"),
        Row(id=2, name="thuy tien", gender="Female"),
        Row(id=3, name="pham thoai", gender="Non-binary")
    ]
)

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("gender", StringType(), True)
])

df = spark.createDataFrame(rdd, schema).show()

