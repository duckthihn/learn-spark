from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]") #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

nums = [1,2,3,4,5,6,7,8,9,10]
# [1,4,9,16,25,36,...]
rdd = sc.parallelize(nums)

square_rdd = rdd.map(lambda x: x**2)
# [4, 5, 6, 7, 8, 9, 10]
filter_rdd = rdd.filter(lambda x: x > 3)

# [[1,2], [2,4], ...]
flatmap_rdd = rdd.flatMap(lambda x: [[x, x*2]])

print(square_rdd.collect())
print(filter_rdd.collect())
print(flatmap_rdd.collect())

