import json
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,DataFrameWriter,DataFrameReader
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum, lit

sparkClassPath = '/usr/local/spark/jars/postgresql-42.2.5.jar'

def getConfig():
    with open("secret.json", "r") as f:
        jsonstr = f.read()
        conf = json.loads(jsonstr)
        return conf

def getSparkConf(config):
    #set config 
    conf = SparkConf()
    conf.setAppName('s1yelp')
    conf.set('spark.jars', 'file:%s' % sparkClassPath)
    conf.set('spark.executor.extraClassPath', sparkClassPath)
    conf.set('spark.driver.extraClassPath', sparkClassPath)
    conf.set('spark.master', config["spark"]["master_url"])
    return conf

def getPostgresProps(config):
    #set psql properties
    props = {
        "user": config["postgres"]["user"],
        "password": config["postgres"]["password"],
        "driver": "org.postgresql.Driver",
    }
    return props

config = getConfig()
sc = SparkContext(conf=getSparkConf(config))
sqlContext = SQLContext(sc)

def getdf(config): 
    #filter yelp dataset
    yelp_business = sqlContext.read.json(config["s3"]["yelpurl"])
    yelp_business_f = yelp_business['name', 'latitude', 'longitude',
                                    'stars', 'review_count', 'address', 
                                    'city', 'state']
    return yelp_business_f

def wrtie_to_psql(yelp_business_f, config):
    #write to psql
    yelp_business_f = getdf(config)
    url = "jdbc:postgresql://localhost/postgres"
    my_writer = DataFrameWriter(yelp_business_f)
    table = 'y_business'
    mode = 'overwrite'
    props = getPostgresProps(config)
    my_writer.jdbc(url, table, mode, props)

