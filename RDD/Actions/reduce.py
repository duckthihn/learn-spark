from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]") #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

nums_rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)

"""
if numSlices = 2
[1,2,3,4,5] [3,3,4,5] [6,4,5] [10,5] [15]
[6,7,8,9,10] [13,8,9,10] [21,9,10] [30,10] [40] 
==> [15,40] -> [55]

if numSlices = 3
[1,2,3] [3,3] [6]
[4,5,6] [9,6] [15]
[7,8,9,10] [15,9,10] [24,10]
==> [6,15,34] -> [55]
"""

def sum(v1: int, v2: int) -> int:
    print(f"v1: {v1}, v2: {v2} => ({v1 + v2})")
    return v1 + v2

print(nums_rdd.glom().collect())
print(nums_rdd.getNumPartitions())
print(nums_rdd.reduce(sum))