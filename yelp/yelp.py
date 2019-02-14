import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrameWriter, DataFrameReader
from pyspark.sql.types import *
from pyspark.sql.functions import *
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine
from geoalchemy2 import Geography

def get_config():
    with open("../config.json", "r") as f:
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
    '''set postgres properties'''
    props = {
        "user": config["postgres"]["user"],
        "password": config["postgres"]["password"],
        "driver": "org.postgresql.Driver",
    }
    return props

env = "development"

def getdf(sql_context, config):
    '''filter yelp dataset'''
    df = sql_context.read.json(config["yelp"]["s3"])
    return df

config = get_config()
spark_conf = get_spark_conf(config)
sc = SparkContext(conf=spark_conf)

sql_context = SQLContext(sc)

raw_df = getdf(sql_context, config)

def get_category_set(raw_df):
    '''split categories, filter null and return unique category value'''
    def split_categories(row):
        if row.categories != None:
            return row.categories.split(", ")
        return []
    
    raw_categories = raw_df.rdd.map(split_categories).collect()
    category_set = set()
    for row in raw_categories:
        category_set.update(row)
    return category_set

all_categories = list(get_category_set(raw_df))

def write_to_pg(df, table, config):
    '''write to postgres'''
    url = config["postgres"][env]["jdbc"]
    props = get_pg_props(config)
    df.write.jdbc(url=url, table=table, mode='overwrite', properties=props)

def write_category_df_to_pg(all_categories, config):
    '''write all categories to postgres, return category dataframe for further use'''
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False)
    ])
    df = sql_context.createDataFrame(zip(range(len(all_categories)), all_categories), schema)
    write_to_pg(df, "categories", config)
    return df

category_df = write_category_df_to_pg(all_categories, config)

def write_yelp_df_to_pg(raw_df, config):
    '''filter yelp dataset and write to postgres, return a dataframe for further use'''
    selected_columns = ["id", "name", "latitude", "longitude", "stars", "review_count", "address", "city", "state"]
    df = raw_df.withColumn("id", monotonically_increasing_id())
    write_to_pg(df[selected_columns], "yelp", config)
    return df[selected_columns + ["categories"]]

def write_yelp2category_to_pg(yelp_df, category_df, config):
    '''write yelp id, category pair to postgres'''
    def categories_to_ids(row):
        '''
        split category column, then create a zip object that one yelp id corresponds to one category each time
        eg: (1, 'Restaurant'),(1, 'Italian'), (1, 'Food')
        '''
        categories = []
        if row["categories"] != None:
            categories = row["categories"].split(", ")
        return zip([row.id] * len(categories), [category_dict[cat] for cat in categories])    
    
    yelp_df = write_yelp_df_to_pg(raw_df, config)
    #create a dictionary of all unique categories
    category_dict = {}
    for row in category_df.collect():
        category_dict[row.name] = row.id
        
    yelp2cat_rdd = yelp_df.rdd.flatMap(categories_to_ids)
    schema = StructType([
        StructField("yelp_id", LongType(), False),
        StructField("category_id", IntegerType(), False)
    ])
    #create a dataframe of category item correspond to yelp id
    yelp2cat_df = sql_context.createDataFrame(yelp2cat_rdd, schema)
    
    #write dataframe to postgres
    write_to_pg(yelp2cat_df, "yelp2category", config)
    return yelp2cat_df

yelp_df = write_yelp_df_to_pg(raw_df, config)
write_yelp2category_to_pg(yelp_df, category_df, config)

def write_yelp_geo_to_pg(yelp_df):
    '''write yelp location to db yelp geo location table'''
    def insertion_dict(row):
        '''filter null'''
        if row.longitude != None and row.latitude != None:
            return {"yelp_id": row.id, "location": f'POINT({row.longitude} {row.latitude})'}
        return None
    
    metadata = MetaData()
    geo_table = Table('yelp_geo', metadata,
        Column('yelp_id', Integer, primary_key=True),
        Column('location', Geography('POINT', srid=4326))
    )
    engine = create_engine(config["postgres"][env]["url"])
    conn = engine.connect()
    conn.execute(geo_table.insert(),
                 yelp_df.rdd.map(insertion_dict).filter(lambda d: d != None).collect())

write_yelp_geo_to_pg(yelp_df)





