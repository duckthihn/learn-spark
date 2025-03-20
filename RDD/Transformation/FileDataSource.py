from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set() #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

rdd = sc.textFile("/home/duckthihn/PycharmProjects/DE-ETL/RDD/data/data.txt")

print(rdd.collect())
print(rdd.getNumPartitions())