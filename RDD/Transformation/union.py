from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]") #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

rdd1 = sc.parallelize([
    { "name": "Amy", "age": 20 },
    { "name": "Bob", "age": 22 },
    { "name": "Cindy", "age": 30 }
])

rdd2 = sc.parallelize([1,2,3,4,5,6])

rdd3 = rdd1.union(rdd2)

print(rdd3.collect())

