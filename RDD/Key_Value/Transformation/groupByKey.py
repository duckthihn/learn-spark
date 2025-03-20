from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set("spark.executor.memory", "4g")
#setMaster() lay bao nhieu core

sc = SparkContext(conf = conf)

rdd = sc.parallelize(["kakashi", "naruto", "conan", "luffy", "satoru"])

keyValue = rdd.map(lambda x: (len(x), x))

groupByKey = keyValue.groupByKey()

for key, value in groupByKey.collect():
    print(key, list(value))