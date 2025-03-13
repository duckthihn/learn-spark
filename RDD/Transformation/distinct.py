from intake.utils import no_duplicate_yaml
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL-102").setMaster("local[*]") #setMaster() lay bao nhieu core #

sc = SparkContext(conf = conf)

data = ["one",1,2,"two",1,2,"three",4,5,6]

no_duplicate_rdd = sc.parallelize(data).distinct()

print(no_duplicate_rdd.collect())