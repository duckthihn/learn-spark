from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set("spark.executor.memory", "4g")
#setMaster() lay bao nhieu core

sc = SparkContext(conf = conf)

table1 = sc.parallelize([("kakashi", 10), ("naruto", 20), ("conan", 30)])

# lookup
print(table1.lookup("kakashi"))
print(table1.lookup("abc"))

# count
print(dict(table1.countByKey()))