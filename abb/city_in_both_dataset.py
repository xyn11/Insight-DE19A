import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import csv

def yelp_city():
        '''
        get yelp cities from original file
        '''
        df = pd.read_json('./yelp/yelp_academic_dataset_business.json', lines = True)
        city = df.city.unique()
        city = city.tolist()
        yelp_city = [x.lower() for x in city]
        yelp_city = set(yelp_city)  
        return yelp_city

def abb_link():
        '''
        scrape airbnb download links from dataset website
        '''
        url = requests.get('http://insideairbnb.com/get-the-data.html').text
        soup = BeautifulSoup(url)
        address = []
        for link in soup.find_all('a'):
                address.append(link.get('href'))
        return address

def abb_city(address):
        '''
        process airbnb link data, output unique cities
        '''
        address_f = []
        for i in range(len(address)):
                link = address[i]
                if link != None and ('listings.csv' in link) and ('visualisations' not in link):
                        address_f.append(link)
        city_set = set()
        for i in range(len(address_f)):
                link = address_f[i]
                tmp = link.split('/')
                city = tmp[-4]
                city_set.add(city)
        return list(city_set)  

def get_city(abb_city, yelp_city):
        '''
        out put unique cities in abb dataset
        '''
        city = []
        for c in abb_city:
                if c in yelp_city:
                        city.append(c)
        return city

def output_txt(records, filename):
        '''
        output txt file of cities in both dataset
        '''
        with open(filename, 'w+') as f:
                for item in records:
                        f.write(item[i]+';')

def main():
        address = abb_link()
        abb_city = abb_city(address)
        yelp_city = yelp_city()
        records = get_city(abb_city, yelp_city)

if __name__ == '__main__':
    main()
