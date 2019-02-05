from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
conf.setMaster('spark://ec2-35-155-195-242.us-west-2.compute.amazonaws.com:7077')
conf.setAppName('spark-basic')
sc = SparkContext(conf=conf)

def mod(x):
    return (x, x%2)

rdd = sc.parallelize(range(1000)).map(mod).take(10)
print rdd