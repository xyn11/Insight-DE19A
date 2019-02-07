import json
import tempfile
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrameWriter, DataFrameReader
from pyspark.sql.types import *
from pyspark.sql.functions import *

def get_config():
    with open("secret.json", "r") as f:
        jsonstr = f.read()
        conf = json.loads(jsonstr)
        return conf

def get_spark_conf(config):
    '''set config'''
    conf = SparkConf()
    conf.setAppName('yelp')
    conf.set('spark.master', config["spark"]["master_url"])
    return conf

def get_pg_props(config):
    '''set psql properties'''
    props = {
        "user": config["postgres"]["user"],
        "password": config["postgres"]["password"],
        "driver": "org.postgresql.Driver",
    }
    return props

def getdf(sql_context, config):
    '''filter yelp dataset'''
    yelp_business = sql_context.read.json(config["s3"]["yelpurl"])
    yelp_business_f = yelp_business[['name', 'latitude', 'longitude',
                                    'stars', 'review_count', 'address',
                                     'city', 'state','categories']]
    yelp_business_f.printSchema()
    return yelp_business_f

def write_to_pg(yelp_business_f, config):
    '''write to psql'''
    url = "jdbc:postgresql://10.0.0.14/xyn"
    table = 'y_business'
    props = get_pg_props(config)
    yelp_business_f.write.jdbc(url=url, table=table, mode='overwrite', properties=props)

def main():
    config = get_config()
    spark_conf = get_spark_conf(config)
    sc = SparkContext(conf=spark_conf)
    sql_context = SQLContext(sc)
    yelp_business_f = getdf(sql_context, config)
    write_to_pg(yelp_business_f, config)

if __name__ == '__main__':
    main()