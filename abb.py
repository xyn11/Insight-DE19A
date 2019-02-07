import json
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
    conf.setAppName('abb_t')
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
    '''filter abb dataset'''
    toronto_listings = sql_context.read.format('com.databricks.spark.csv').options(header='true').load(config["s3"]["abb_toronto_listings"])
    toronto_listings_f = toronto_listings['id', 'latitude', 'longitude',
                                    'price', 'number_of_reviews']
    return toronto_listings_f

def write_to_pg(toronto_listings_f, config):
    '''write to psql'''
    url = "jdbc:postgresql://10.0.0.14/xyn"
    table = 'toronto_listings'
    mode = 'overwrite'
    props = get_pg_props(config)
    toronto_listings_f.write.jdbc(url, table, mode, props)

def main():
    config = get_config()
    spark_conf = get_spark_conf(config)
    sc = SparkContext(conf=spark_conf)
    sql_context = SQLContext(sc)
    toronto_listings_f = getdf(sql_context, config)
    write_to_pg(toronto_listings_f, config)

if __name__ == '__main__':
    main()