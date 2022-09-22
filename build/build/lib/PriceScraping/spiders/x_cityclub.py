# -*- coding: utf-8 -*-
import re
import io
import json
import time

import scrapy
import random
import datetime
import pandas as pd
import numpy as np

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request
import logging
logging.getLogger("snowflake.connector.network").disabled = True

url=variables.c_url
allowed_domains=variables.c_allowed_domains
engine = variables.engine

site_dtype = variables.site_dtype
site_headers = variables.site_headers

keep_s_list = variables.cc_s_keep_list
alter_s_list = variables.cc_s_alter_list

Date = datetime.datetime.today().strftime('%Y-%m-%d')

Testmode = True
N = 10 if Testmode else 10**8

class CityClubSiteSpider(scrapy.Spider):
    name='cityclub-sites'
    site_out=[]

    allowed_domains=allowed_domains
    start_urls=[url]

    def start_requests(self):
        variables.c_payload['operationName']="LocationsQuery"
        variables.c_payload['query']=variables.c_qloc
        bdy=json.dumps(variables.c_payload)

        for u in self.start_urls:
          yield Request(u, 
                          method='POST', 
                          body=bdy,
                          headers=variables.c_headers,
                          callback=self.parse_detail)

    def parse_detail(self, response):
      json_response=json.loads(response.text)
      venues=json_response['data']['merchant']['locations']

      for venue in venues:
        self.site_out.append(venue)
        yield venue

class CityClubSiteSpiderFormatted(scrapy.Spider):
    name='cityclub-sites-formatted'
    site_out=[]

    allowed_domains=allowed_domains
    start_urls=[url]

    def start_requests(self):
        variables.c_payload['operationName']="LocationsQuery"
        variables.c_payload['query']=variables.c_qloc
        bdy=json.dumps(variables.c_payload)
        print(alter_s_list)

        for u in self.start_urls:
          yield Request(u,
                          method='POST',
                          body=bdy,
                          headers=variables.c_headers,
                          callback=self.parse_detail)

    def parse_detail(self, response):
      json_response=json.loads(response.text)
      venues=json_response['data']['merchant']['locations']

      for venue in venues:
        venue['Latitude'] = venue['coords']['latitude']
        venue['Longitude'] = venue['coords']['longitude']
        venue['Address Line 1'] = venue['address']['street']
        venue['Company'] = 'City Club'
        venue['Format'] = 'City Club'
        venue['Country'] = venue['address']['region']
        venue['County'] = venue['address']['locality']
        venue['Date'] = Date
        venue['Site Number'] = f'''CC-{venue['id']}'''
        venue['Other Town'] = venue['address']['locality']
        venue['Postcode'] = venue['address']['postalCode']
        venue['Status'] = 'OPEN'
        venue['Town'] = 'N/A'

        venue = helpers.rename_items(venue, alter_s_list)
        venue = helpers.keep_items(venue, keep_s_list)

        self.site_out.append(venue)
        yield venue
    def closed(self, reason):
        if self.site_out:
            Site_Data = pd.json_normalize(self.site_out)
            Site_Data = Site_Data[site_headers]
            print(Site_Data.head(10))
            Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)
            if Site_Data is not None:
                database_Output = 'Competitor_Houselist'
                Site_Data.to_sql(name=database_Output, con=engine, if_exists='append', index=False, chunksize=16000,
                                 dtype=site_dtype)
            else:
                print("Empty Dataframe")
                pass

class CityClubMenuSpider(scrapy.Spider):
    name='cityclub-menus'
    menu_out=[]

    allowed_domains=allowed_domains
    start_urls=[url]

    def parse(self, response):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))

        for site in job.items.iter(count=N):
          pubName=site['name']
          pubCode=site['id']

          variables.c_payload['operationName']="OrderingQuery"
          variables.c_payload['query']=variables.c_qmen
          variables.c_payload['variables']={"location": pubCode}
          bdy=json.dumps(variables.c_payload)
        
          time.sleep(random.randint(0,1))
          yield Request(response.url, 
                          method='POST', 
                          body=bdy,
                          headers=variables.c_headers,
                          callback=self.parse_detail)

    def parse_detail(self, response):
      json_response=json.loads(response.text)
      menus=json_response['data']['ordering']['menu']

      for menu in menus:
        print(menu)

