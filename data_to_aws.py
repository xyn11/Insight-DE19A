from bs4 import BeautifulSoup
import requests
from urlparse import urljoin
from os.path import basename

'''extract useful url from origin website'''
url = requests.get('http://insideairbnb.com/get-the-data.html').text
soup = BeautifulSoup(url)
address_list = []
for a in soup.select(i for i in ["a[href$ = .csv]", "a[href$ = .csv.fz]", "a[href$ = .geojson]"):
    address_list.append(a)

#write file    
for link in address_list:
    with open(basename(link), "w") as f:
        f.writelines(requests.get(link))


#from ec2 to s3
aws s3 sync your-dir-name/file-name s3://your-s3-bucket-name/folder-name/file-name