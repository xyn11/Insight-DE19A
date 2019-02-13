import argparse
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrameWriter, DataFrameReader
from pyspark.sql.types import *
from pyspark.sql.functions import *

env = "development"

def get_config():
    with open("../config.json", "r") as f:
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

def getdf(sql_context, config, city):
    '''filter abb dataset'''

    df = sql_context\
         .read\
         .format('csv')\
         .options(header='true')\
         .load(config["abb"][city]["s3"])
    return df

def append_to_pg(df, config):
    '''write to psql'''
    url = config["postgres"][env]["jdbc"]
    props = get_pg_props(config)
    df.write.jdbc(url, table="abb", mode="append", properties=props)

selected_columns = ["name", "city", "latitude", "longitude", "price", "number_of_reviews"]

def get_city_df(sql_context, config, city):
    df = getdf(sql_context, config, city=city)
    city_listings = df.withColumn("latitude", df["latitude"]\
                      .cast(DoubleType()))\
                      .withColumn("longitude", df["longitude"].cast(DoubleType()))\
                      .withColumn("price", df["price"].cast(IntegerType()))\
                      .withColumn("number_of_reviews", df["number_of_reviews"].cast(IntegerType()))\
                      .withColumn("city", lit(city))[selected_columns]
    return city_listings

def main(city):
    config = get_config()
    spark_conf = get_spark_conf(config)
    sc = SparkContext(conf=spark_conf)
    sql_context = SQLContext(sc)
    df = get_city_df(sql_context, config, city)
    append_to_pg(df, config)

def parse_city_from_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("city")
    args = parser.parse_args()
    return args.city

if __name__ == "__main__":
    city = parse_city_from_args()
    main(city)
