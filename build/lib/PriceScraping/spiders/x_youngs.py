# -*- coding: utf-8 -*-
import re
import io
import json
import time
import numpy as np
import scrapy
import random
import datetime
import pandas as pd
import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables
import hashlib

import urllib.parse

from pprint import pprint
from scrapy.http import Request
import logging
logging.getLogger("snowflake.connector.network").disabled = True

s_url=variables.y_urlS
m_url=variables.y_urlM
allowed_domains=variables.y_allowed_domains

Testmode = False
N = 1 if Testmode else 10**8

Date = datetime.datetime.today().strftime('%Y-%m-%d')

class YoungsSiteSpider(scrapy.Spider):
    name='youngs-sites'
    site_out=[]

    start_urls=[s_url]

    def start_requests(self):
        for u in self.start_urls:
            u=u+"locations?per_page=999&summary=1" #+"&services=order_to_table"
            yield Request(u,
                            method="GET",
                            headers=variables.y_headers,
                            callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        venues=json_response['locations']

        for venue in venues:
            self.site_out.append(venue)
            yield venue

# class YoungsSiteSpiderFormatted(scrapy.Spider):
#     name='youngs-sites-formatted'
#     site_out=[]
#
#     start_urls=[s_url]
#
#     def start_requests(self):
#         for u in self.start_urls:
#             u=u+"locations?per_page=999&summary=1" #+"&services=order_to_table"
#             yield Request(u,
#                             method="GET",
#                             headers=variables.y_headers,
#                             callback=self.parse_detail)
#
#     def parse_detail(self, response):
#         json_response=json.loads(response.text)
#         venues=json_response['locations']
#         venues = venues
#
#         for venue in venues:
#             yield scrapy.Request(f'''https://api.postcodes.io/postcodes?lon={venue['geo'][0]}&lat={venue['geo'][1]}''',
#                           method='GET',
#                           callback=self.detail,
#                           meta={'Site Name': venue['title'], 'Given ID': venue['_id'], 'Address Line 1': venue['address'],
#                                 'Latitude': venue['geo'][1], 'Longitude': venue['geo'][0]})
#             time.sleep(random.randint(1, 3))
#
#
#     def detail(self, response):
#         json_response = json.loads(response.text)
#
#         Data = response.meta
#         Data['Postcode'] = json_response['result'][0]['postcode']
#         Data['Company'] = 'Youngs'
#         Data['Country'] = json_response['result'][0]['country']
#         Data['County'] = json_response['result'][0]['primary_care_trust']
#         Data['Date'] = Date
#         Data['Format'] = 'Youngs'
#         Data['Other Town'] = json_response['result'][0]['parliamentary_constituency']
#         Data['Site Number'] = f'''YGS-{Data['Given ID']}'''
#         Data['Telephone Number'] = 'N/A'
#         Data['Town'] = json_response['result'][0]['parliamentary_constituency']
#         Data['Status'] = 'OPEN'
#
#         Data = helpers.keep_items(Data, variables.site_headers)
#         self.site_out.append(Data)
#         yield Data
#
#     def closed(self, reason):
#         if self.site_out:
#             Site_Data = pd.json_normalize(self.site_out)
#             Site_Data = Site_Data[variables.site_headers]
#             Site_Data = Site_Data.astype(str)
#             print(Site_Data.head(20).to_string())
#             Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)
#             if Site_Data is not None:
#                 database_Output = 'Competitor_Houselist'
#                 Site_Data.to_sql(name=database_Output, con=variables.engine, if_exists='append', index=False, chunksize=16000, dtype=variables.site_dtype)
#             else:
#                 print("Empty Dataframe")
#                 pass

class YoungsSiteSpiderFormatted(scrapy.Spider):
    name = 'youngs-sites-formatted'

    allowed_domains=allowed_domains
    start_urls=[s_url]

    out = pd.DataFrame()

    def start_requests(self):
        for u in self.start_urls:
            u=u+"locations?per_page=999"+"&services=order_to_table"
            yield Request(u,
                            method="GET",
                            headers=variables.y_headers,
                            callback=self.parse)


    def parse(self, response):
        json_response = json.loads(response.text)
        for site in json_response['locations']:

            site['Address Line 1'] = site['address']['address']
            site['Format'] = 'Youngs'
            site['Company'] = 'Youngs'
            site['Country'] = site['address']['country']
            site['Date'] = Date
            site['Latitude'] = site['geo'][1]
            site['Longitude'] =site['geo'][0]
            try:
                site['Town'] = site['address']['town']
            except:
                site['Town'] = site['address']['summary'].split(",")[1]
            site['Other Town'] = site['Town']
            site['Postcode'] = site['address']['postcode']
            site['Site Number'] = f'''YGS-{site['_id']}'''
            site['Site Name'] = site['title']
            site['Status'] = site['state']
            site['Given ID'] = site['tenantId']
            site['Telephone Number'] = site['contacts']['phone']

            site = helpers.keep_items(site, variables.site_headers)

            yield site
            self.out = self.out.append(pd.json_normalize(site))

    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(10).to_string())
            out = self.out[variables.site_headers]
            database_Output = 'competitor_sites'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                       dtype=variables.price_dtype)

class YoungsPriceSpider(scrapy.Spider):
    name='youngs-prices-formatted'
    down = 'youngs-sites-formatted'
    data_out = []

    allowed_domains=allowed_domains
    start_urls=[m_url]

    def start_requests(self):
        job=helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))

        for site in job.items.iter(count=N):
            url = f'''https://menu.pepperhq.com/menu?tenantId={site['Given ID']}&locationId={site['Site Number'].split("-")[1]}&scenario=ORDER_TO_TABLE'''
            yield scrapy.Request(url,
                                 method='GET',
                                 headers=variables.y_headers,
                                 callback=self.parse_detail,
                                 meta={'Site Name': site['Site Name'],
                                       'Site Number': site['Site Number']})
            time.sleep(random.randint(1, 3))

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        prices = json_response['categories']

        for menus in prices:
            for products in menus['products']:
                products['Site Name'] = response.meta['Site Name']
                products['Site Number'] = response.meta['Site Number']
                products['Company'] = 'Youngs'
                products['Date'] = Date
                products['Format'] = 'Youngs'
                products['Menu ID'] = menus['id']
                products['Menu Name'] = re.sub(r'\W+', '', menus['title'])
                products['Price'] = products['price']
                try:
                    products['Product Name'] = products['productGroupName']
                    products['Portion Size'] = products['title']
                except:
                    products['Product Name'] = products['title']
                    products['Portion Size'] = 'Standard'

                products['Product ID'] = hashlib.md5(products['Product Name'].encode()).hexdigest()

                products = helpers.keep_items(products, variables.price_headers)

                self.data_out.append(products)
                yield products

    def closed(self, reason):
        if self.data_out:
            Price_Data = pd.json_normalize(self.data_out)
            Price_Data = Price_Data[variables.price_headers]
            Price_Data.astype(str).apply(lambda x: x.str.encode('ascii', 'ignore').str.decode('ascii'))
            print(Price_Data.head(10).to_string())
            if Price_Data is not None:
                database_Output = 'competitor_prices'
                Price_Data.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                           chunksize=16000,
                           dtype=variables.price_dtype_C)
            else:
                print("Empty Dataframe")
                pass