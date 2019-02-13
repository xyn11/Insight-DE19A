import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrameWriter, DataFrameReader
from pyspark.sql.types import *
from pyspark.sql.functions import *

from sqlalchemy import Table, Column, Integer, Float, String, MetaData, create_engine
from geoalchemy2 import Geography
from sqlalchemy.sql import select
from sqlalchemy import func

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

metadata = MetaData()

yelp_table = Table('yelp', metadata,                   
                   Column('id', Integer, primary_key=True),
                   Column('name', String),
                   Column('latitude', Float),
                   Column('longitude', Float),
                   Column('stars', Float),
                   Column('review_count', Integer),
                   Column('address', String),
                   Column('city', String),
                   Column('state', String),
                   keep_existing=True)

categories_table = Table('categories', metadata,
                         Column('id', Integer, primary_key=True),
                         Column('name', String),
                         keep_existing=True)


yelp2category_table = Table('yelp2category', metadata,
                            Column('yelp_id', Integer),
                            Column('category_id', String),
                            keep_existing=True)

yelp_geo_table = Table('yelp_geo', metadata,
                        Column('yelp_id', Integer),
                        Column('location', Geography(geometry_type='POINT', srid=4326)),
                        keep_existing=True)

abb_table = Table('abb', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('name', String),
                  Column('latitude', Float),
                  Column('longitude', Float),
                  Column('price', Integer),
                  Column('number_of_reviews', Integer),
                  Column('city', String),
                  keep_existing=True)            

distance_threshold = 500

score_dict = {10: 1000, 9: 100, 8: 10, 7:1}

def compute_listing_score(conn, listing, city, category):
    def compute_score(selection_result):
        score = 0
        for row in selection_result:
            doublestar = int(row.stars*2)
            if doublestar in score_dict:
                score += score_dict[doublestar]
        return score    
    
    if listing.longitude == None or listing.latitude == None:
        return 0
    selection = select([yelp_table.c.stars])\
                .select_from(yelp_table.join(yelp2category_table, yelp_table.c.id == yelp2category_table.c.yelp_id)\
                                       .join(categories_table, yelp2category_table.c.category_id == categories_table.c.id)\
                                       .join(yelp_geo_table, yelp_geo_table.c.yelp_id == yelp_table.c.id))\
                .where(yelp_table.c.city == city)\
                .where(categories_table.c.name == category)\
                .where(func.ST_Distance(f'POINT({listing.longitude} {listing.latitude})',
                                        yelp_geo_table.c.location) <= distance_threshold)\
                .where(yelp_table.c.stars > 3)  
    
    result = conn.execute(selection)
    return compute_score(result)

def write_score_table(conn, listings, city, category, debug=True):
    def prepare_score_rows():
        rows = []
        progress = 0
        for listing in listings:
            score = compute_listing_score(conn, listing, city, category)
            rows.append({"abb_id": listing.id, "score": score})
            progress += 1
            if debug and progress % 200 == 0:
                print("progress = ", progress)
        return rows
    
    rows = prepare_score_rows()
    table_name = f"abb_{category.lower()}"
    score_table = Table(table_name, metadata,
                        Column('abb_id', Integer, primary_key=True),
                        Column('score', Integer),
                        keep_existing=True)
    conn.execute(score_table.insert(), rows)

config = get_config()

def run_city_category(city_category_pair):
    engine = create_engine(config["postgres"][env]["url"])
    conn = engine.connect()
    city, category = city_category_pair
    
    selection = select([abb_table.c.id, abb_table.c.name, abb_table.c.longitude, abb_table.c.latitude])\
                .select_from(abb_table)\
                .where(abb_table.c.city == city)
    listings = conn.execute(selection)
    write_score_table(conn, listings, city, category)

def main():
    spark_conf = get_spark_conf(config)
    sc = SparkContext(conf=spark_conf)

    pairs = []
    for city in ["Oakland", "Sevilla", "Boston", "Austin"]:
        for category in ["Chinese", "Italian", "Shopping", "Restaurants", "Pubs"]:
            pairs.append((city, category))

    rdd = sc.parallelize(pairs)
    rdd.map(run_city_category).collect()

if __name__ == "__main__":
    main()
