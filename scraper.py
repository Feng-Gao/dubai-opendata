#!/usr/bin/python
# -*- coding: utf-8 -*-

#a quick and dirty script to scrape/harvest resource-level metadata records
#the original purpose of this work is to support the ongoing international city open data index project led by SASS

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import re
import scraperwiki
import datetime

#we need random ua to bypass website security check
ua = UserAgent()
headers = {'User-Agent':ua.random}

#dubai does not have a full list of dataset. instead, it ask you first check a list of category or orgnaization
#we choose to go over org list, from there, we can access the real data list.
#we need to create a full url list to data list page then iterate that list to crawl back all data info

org_url = 'https://www.dubaipulse.gov.ae/data/organisation'

result = requests.get(org_url,headers=headers)
soup = BeautifulSoup(result.content,features='lxml')

all_div = soup.find_all(attrs={'class':'item-list'})
dataurl_list = []
for div in all_div:
  all_a = div.find_all('a')
  for a in all_a:
    dataurl_list.append({'name':a.text,'url':"https://www.dubaipulse.gov.ae"+a['href']+'?result_per_page_service=100'})

for l in dataurl_list:
    print(l['url'])
    #we are now on the organization-service level data list page
    result = requests.get(l['url'],headers=headers)
    soup = BeautifulSoup(result.content,features='lxml')
    #get all data sets
    package_blocks = soup.find_all(attrs={'class':'data-item'})
    for p in package_blocks:
        package_name = p.find(attrs={'class':'title'}).a.text.strip()
        print(package_name)
        package_url = "https://www.dubaipulse.gov.ae"+p.a['href']
        print(package_url)
        package_topics = l['name']
        #if the data contains api, then it return a string 'DATA API' otherwise it is None
        try:
            package_api = p.find(attrs={'class':'sharedDataset'}).text.strip()
        except:
            package_api = ''
        
        package_updated = p.find(attrs={'class':'update-date'}).text.strip().split(':')[1]
        
        #we are now on the actual dataset detail page
        result = requests.get(package_url,headers=headers)
        soup = BeautifulSoup(result.content,features='lxml')

        package_org = soup.find(attrs={'class':'dataset-author'}).a.text.strip()
        
        package_desc = '"'+soup.find(attrs={'class':'additional-desc'}).text.strip()+'"'
        
        package_provenance = '"'+soup.find(string='Data Provenance').parent.parent.td.text.strip()+'"'
        
        package_tags = '|'.join([x.text.strip() for x in soup.find(attrs={'class':'dataset-keywords'}).td.find_all('span')])
        
        package_frequency_source = soup.find(string='Frequency of Update on Source').parent.parent.parent.td.text.strip()
       
        package_frequency_sdp = soup.find(string='Frequency of Update to SDP').parent.parent.parent.td.text.strip()
        
        package_column = str(len(soup.find_all(attrs={'class':'showmoreDataLoop'})))
        

        resource_blocks = soup.find_all(attrs={'class':'file-item'})
        package_resource_num = str(len(resource_blocks))

        for r in resource_blocks:
            resource_name = r.find(attrs={'class':'file-title'}).a.text.strip()
            print(resource_name)
            resource_meta_url = "https://www.dubaipulse.gov.ae"+r.find(attrs={'class':'file-title'}).a['href']
            print(resource_meta_url)
            resource_file_url = "https://www.dubaipulse.gov.ae"+r.find(attrs={'class':'file-action'}).a['href']

            result = requests.get(resource_meta_url,headers=headers)
            soup = BeautifulSoup(result.content,features='lxml')
            #because text formating issue, the string itself can not be exactly matched so we use re to find the element
            resource_created = soup.find(string=re.compile('Created')).parent.parent.td.text.strip()
            
            resource_updated = soup.find(string=re.compile('Last updated')).parent.parent.td.text.strip()
            
            resource_format = soup.find(string=re.compile('Format')).parent.parent.td.text.strip()
            
            row = package_url +','+ package_name+','+package_desc+','+package_org+','+package_topics \
                +','+package_tags+','+','+package_frequency_sdp+','+package_updated+','+package_api\
                +','+package_column+','+package_resource_num+','+package_provenance\
                +','+package_frequency_source+','+resource_meta_url+','+resource_file_url+','+resource_name\
                +','+resource_created+','+resource_updated+','+resource_format+'\n'
            print(row)
            package_dict = {
                    'today':datetime.date.today().strftime("%m/%d/%Y"),
                    'url':package_url,
                
                    'name':package_name,
                    'desc':package_desc,
                    'org':package_org,
                    'topics':package_topics,
                    'tags':package_tags,
                    'frequency':package_frequency_sdp,
                    'updated':package_updated,
                    'api':package_api,
                    'column':package_column,
                    'resource_count':package_resource_num,
                    'provenance':package_provenance,
                    'frequency-source':package_frequency_source,
                    'resource_meta_url':resource_meta_url,
                    'resource_file_url':resource_file_url,
                    'resource_name':resource_name,
                    'resource_created':resource_created,
                    'resource_updated':resource_updated,
                    'resource_format':resource_format,
                    
          }
            scraperwiki.sqlite.save(unique_keys=['today','url'],data=package_dict)

        print('****************end---'+package_name+'---end****************')
