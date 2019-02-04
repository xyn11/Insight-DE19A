import json
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,DataFrameWriter,DataFrameReader
from pyspark.sql.types import *
from pyspark.sql.functions import *

sparkClassPath = '/usr/local/spark/jars/postgresql-42.2.5.jar'

def getConfig():
    with open("secret.json", "r") as f:
        jsonstr = f.read()
        conf = json.loads(jsonstr)
        return conf

def getSparkConf(config):
    '''set config''' 
    conf = SparkConf()
    conf.setAppName('s1abb')
    conf.set('spark.jars', 'file:%s' % sparkClassPath)
    conf.set('spark.executor.extraClassPath', sparkClassPath)
    conf.set('spark.driver.extraClassPath', sparkClassPath)
    conf.set('spark.master', config["spark"]["master_url"])
    return conf

def getPostgresProps(config):
    '''set psql properties'''
    props = {
        "user": config["postgres"]["user"],
        "password": config["postgres"]["password"],
        "driver": "org.postgresql.Driver",
    }
    return props

config = getConfig()
sc = SparkContext(conf=getSparkConf(config))
sqlContext = SQLContext(sc)

def set_schema():
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("latitude", FloatType()),
        StructField("longitude", FloatType()),
        StructField("price", IntegerType()),
        StructField("number_of_reviews", IntegerType())
        ])


def getdf(config): 
    '''filter yelp dataset'''
    toronto_listings = sqlContext.read.format('com.databricks.spark.csv')
                            .options(header='true', schema = schema)
                            .load(config["s3"]["abb_toronto_listings"])
    toronto_listings_f = toronto_listings['id', 'latitude', 'longitude',
                                    'price', 'number_of_reviews']
    return toronto_listings_f

def wrtie_to_psql(yelp_business_f, config):
    '''write to psql'''
    yelp_business_f = getdf(config)
    url = "jdbc:postgresql://localhost/postgres"
    my_writer = DataFrameWriter(yelp_business_f)
    table = 'toronto_listings'
    mode = 'overwrite'
    props = getPostgresProps(config)
    my_writer.jdbc(url, table, mode, props)