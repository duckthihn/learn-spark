from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]").set("spark.executor.memory", "4g")
#setMaster() lay bao nhieu core

sc = SparkContext(conf = conf)

table1 = sc.parallelize([("kakashi", 10), ("naruto", 20), ("conan", 30)])
table2 = sc.parallelize([("kakashi", "coach"), ("naruto", "hokage"), ("conan", "detective")])

joinByKey = table1.join(table2).sortByKey(ascending=False)

for result in joinByKey.collect():
    print(result)