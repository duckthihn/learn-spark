from random import Random
import time
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]") #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

data = ["Toan Nguyen-debt", "Hieu-debt", "Duy-debt", "DucAnh-debt"]

rdd = sc.parallelize(data, 2)

# def partition(iterator):
#     rand = Random(int(time.time() * 1000) + Random().randint(0, 1000))
#     return [f"{item}:{rand.randint(0,1000)}" for item in iterator]
#
# result = rdd.mapPartitions(partition)
# print(result.collect())

results = rdd.mapPartitions(
    lambda x : map(
        lambda y : f"{y}:{Random(int(time.time() * 1000) + Random().randint(0, 1000)).randint(0, 1000)}", x
    )
)

print(results.collect())