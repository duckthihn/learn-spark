from pyspark import SparkContext

sc = SparkContext("local[4]", appName="DE-ETL-102")  # set local[*] chia phan vung

data = [1,2,3,4,5]
rdd = sc.parallelize(data)

print(rdd.first())
print(rdd.count())
print(rdd.getNumPartitions())
print(rdd.glom().collect())