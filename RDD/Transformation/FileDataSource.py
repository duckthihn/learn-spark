from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set() #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

rdd = sc.textFile("/home/duckthihn/PycharmProjects/DE-ETL/RDD/Data/data.txt")

print(rdd.collect())
print(rdd.getNumPartitions())