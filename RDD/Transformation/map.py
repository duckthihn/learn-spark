from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]") #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

file_rdd = sc.textFile("/home/duckthihn/PycharmProjects/DE-ETL/RDD/Data/data.txt")

allCaps_rdd = file_rdd.map(lambda line: line.upper())

print(file_rdd.collect()) # collect(): dua data vao list
print(allCaps_rdd.collect())

for line in allCaps_rdd.collect():
    print(line)