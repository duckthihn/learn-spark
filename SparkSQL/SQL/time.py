from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
import re

spark = SparkSession.builder.appName("DateFormatUDF").getOrCreate()

data = [("11/12/2025",), ("27/02/2014",), ("2023.01.09",), ("28-12-2005",)]
df = spark.createDataFrame(data, ["date"])

def extract_date(data_str):
    for delimiter in ['/','.','-']:
        if delimiter in data_str:
            day, month, year = data_str.split(delimiter)
            if len(year) == 4:
                return day, month, year
            else:
                return year, month, day


date_udf = udf(extract_date, StructType([
    StructField("day", StringType(), True),
    StructField("month", StringType(), True),
    StructField("year", StringType(), True)
]))

df = df.withColumn("parsed_date", date_udf(df["date"]))

df = df.select("date", "parsed_date.*")

df.show()