from bs4 import BeautifulSoup
import requests
from urlparse import urljoin
from os.path import basename

'''extract useful url from origin website'''
url = requests.get('http://insideairbnb.com/get-the-data.html').text
soup = BeautifulSoup(url)
address_list = []
'''get all url'''
for link in soup.find_all('a'):
    address_link.append(link.get('href'))
'''filter url that ends with csv, csv.gz, geojson'''
adrress_filtered = []if url.find('/'):
for link in address_list:
        if 'csv' in address_list:
                adrress_filtered.append(link)

#write file 
def download(link, name):   
        for link in address_filtered:
                with open(name, "w") as f:
                        f.writelines(requests.get(link))

def get_name(url):
        name_from_url = url.split('.com/')[1].replace('/','_')


#from ec2 to s3
aws s3 sync your-dir-name/file-name s3://your-s3-bucket-name/folder-name/file-name