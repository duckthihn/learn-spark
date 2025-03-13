from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]") #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

rdd1 = sc.parallelize([1,2,3,4,5,6])
rdd2 = sc.parallelize([4,5,6,7,8,9])

rdd3 = rdd1.intersection(rdd2)

print(rdd3.collect())