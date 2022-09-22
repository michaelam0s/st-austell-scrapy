# -*- coding: utf-8 -*-
import re
import io
import json
import time
import pandas as pd
import numpy as np
import scrapy
import random
from datetime import datetime
import hashlib

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request
import logging
logging.getLogger("snowflake.connector.network").disabled = True

Date = datetime.today().strftime('%Y-%m-%d')

s_url=variables.m_urlS
m_url=variables.m_urlM
allowed_domains=variables.m_allowed_domains

s_keep_list = variables.m_s_keep_list
s_del_list = variables.m_s_del_list
del_p_list = variables.m_del_list
alter_p_list = variables.m_alter_list

del_list = variables.m_del_list

site_dtype = variables.site_dtype
site_headers = variables.site_headers

price_headers = variables.price_headers
price_dtype = variables.price_dtype

engine = variables.engine

Limit = True
Testmode = False
N = 5 if Testmode else 10**8

class MarstonsSitesBulk(scrapy.Spider):
    name = 'marstons-sites'
    site_out = []

    def start_requests(self):
        yield Request(url='https://www.marstonspubs.co.uk/ajax/finder/markers/',
                      headers=variables.m_bulk_headers,
                      callback=self.parse_detail)

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        json_response = json_response['markers']
        for i in json_response[0:N]:
            yield Request(url=f'''https://www.marstonspubs.co.uk/ajax/finder/outlet/?p={i['i']}''',
                          headers=variables.m_bulk_headers,
                          callback=self.parse_response)
            time.sleep(random.randint(1, 3))
    def parse_response(self, response):
        json_response = json.loads(response.text)
        yield json_response

#Bulk call for all sites formatted
class MarstonsSitesBulkFormatted(scrapy.Spider):
    name = 'marstons-sites-formatted'
    down = 'marstons-app-sites-formatted'
    site_out = []

    def start_requests(self):
        yield Request(url='https://www.marstonspubs.co.uk/ajax/finder/markers/',
                      headers=variables.m_bulk_headers,
                      callback=self.parse_detail)

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        json_response = json_response['markers']
        for i in json_response[0:N]:
            yield Request(url=f'''https://www.marstonspubs.co.uk/ajax/finder/outlet/?p={i['i']}''',
                          headers=variables.m_bulk_headers,
                          callback=self.parse_response)
            time.sleep(random.randint(1, 3))
    def parse_response(self, response):
        json_response = json.loads(response.text)
        json_response = json_response['outlets'][0]
        json_response['Address Line 1'] = json_response['address']
        json_response['Company'] = 'Marstons'
        json_response['Country'] = 'UK'
        json_response['County'] = json_response['address'].split(",")[-2]
        json_response['Date'] = Date
        json_response['Format'] = 'Marstons'
        json_response['Site Number'] = f'''MST-{json_response['phc']}'''
        json_response['Other Town'] = json_response['town']
        json_response['Postcode'] = json_response['address'].split(",")[-1]
        json_response['Status'] = 'OPEN'
        json_response['Telephone Number'] = 'N/A'

        json_response = helpers.rename_items(json_response, variables.m_s_alter_list_bulk)
        json_response = helpers.keep_items(json_response, variables.site_headers)

        self.site_out.append(json_response)
        yield json_response

    def closed(self, reason):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        Items = pd.DataFrame()
        for site in job.items.iter(count=N):
            Items = Items.append(site, ignore_index=True)

        print(Items)


        if self.site_out:
            Site_Data = pd.json_normalize(self.site_out)
            Site_Data = Site_Data[variables.site_headers]
            Site_Data = Site_Data.astype(str)
            Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)

            for index, row in Items.iterrows():
                Site_Data = Site_Data[Site_Data['Site Name'] != row['Site Name']]

            Site_Data = Site_Data.append(Items)
            print(Site_Data.head(2000).to_string())

            #     if Site_Data is not None:
        #         database_Output = 'Competitor_Houselist'
        #         Site_Data.to_sql(name=database_Output, con=variables.engine, if_exists='append', index=False, chunksize=16000, dtype=variables.site_dtype)
        #     else:
        #         print("Empty Dataframe")
        #         pass

#app call (limited number of sites ~250)
class MarstonsSite(scrapy.Spider):
    name = 'marstons-app-sites-formatted'
    site_out = []

    allowed_domains = allowed_domains
    start_urls = [s_url]

    def start_requests(self):
        for u in self.start_urls:
            yield Request(u,
                          method="GET",
                          headers=variables.m_headers,
                          callback=self.parse_detail)

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        venues = json_response['venues']

        for site in venues:
            site['Address Line 1'] = site['address']['streetAddress']
            site['Format'] = 'Marstons'
            site['Company'] = 'Marstons'
            site['Country'] = 'UK'
            site['County'] = site['address']['city']
            site['Date'] = Date
            site['Given ID'] = site['_id']
            site['Latitude'] = site['address']['location']['coordinates'][1]
            site['Longitude'] = site['address']['location']['coordinates'][0]
            site['Other Town'] = site['address']['address2']
            site['Postcode'] = site['address']['postCode']
            site['Site Number'] = f'''MST-{site['_id']}'''
            site['Status'] = 'OPEN'
            site['Site Name'] = site['name']
            if site['address']['city'] in site['Site Name']:
                site['Site Name'] = site['Site Name'].replace(site['address']['city'], "")
            if site['isEnabled'] == 'false':
                site['Status'] = 'CLOSED'

            site['Telephone Number'] = 'N/A'
            Town = site['address']['address2']
            if not site['address']['address2']:
                Town = site['address']['city']
            site['Town'] = Town
            site = helpers.delete_items(site, s_del_list)

            self.site_out.append(site)
            yield site

    def closed(self, reason):
        Site_Data = pd.json_normalize(self.site_out)
        Site_Data = Site_Data[site_headers]
        Site_Data = Site_Data.astype(str)
        print(Site_Data.head(5).to_string())
        Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)
        if Site_Data is not None:
            database_Output = 'competitor_sites'
            Site_Data.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000, dtype=site_dtype)
        else:
            print("Empty Dataframe")
            pass

class MarstonsSiteSpider(scrapy.Spider):
    name='marstons-sites-app'
    site_out=[]

    allowed_domains=allowed_domains
    start_urls=[s_url]

    def start_requests(self):
        for u in self.start_urls:
            yield Request(u,
                            method="GET",
                            headers=variables.m_headers,
                            callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        venues=json_response['venues']

        for venue in venues:
            venue['pubName']=venue['name']
            venue['pubCode']=venue['_id']
            yield venue

class MarstonsMenuSpider(scrapy.Spider):
    name = 'marstons-prices-formatted'
    down = 'marstons-sites-app'
    out = pd.DataFrame()
    ot = pd.DataFrame()

    allowed_domains=allowed_domains
    start_urls=[m_url]

    def start_requests(self):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))

        for site in job.items.iter(count=N):
            slug=site['slug']
            urlS = "https://api-cdn.orderbee.co.uk/venues/{0}".format(slug)

            pubName=site['name']
            pubCode=site['_id']

            time.sleep(random.randint(0,2))
            yield Request(urlS,
                            method="GET",
                            headers=variables.m_headers,
                            callback=self.parse_detail,
                            meta={'pubName':pubName,'pubCode':pubCode})

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        menus=json_response['menus']['oat']['categories']

        Data = []

        for menu in menus:
            desc=menu['name']
            menuId = menu['_id']
            for items in menu['subCategories']:
                try:
                    for products in items['items']:
                        products['Site Name'] = response.meta['pubName']
                        products['Site Number'] = f'''MST-{response.meta['pubCode']}'''
                        products['Menu Name'] = desc
                        products['Menu ID'] = hashlib.md5(str(desc).encode()).hexdigest()
                        products['Product ID'] = hashlib.md5(str(products['shortName']).encode()).hexdigest()
                        products['Company'] = 'Marstons'
                        products['Format'] = 'Pub'
                        products['Date'] = Date
                        if "PT" in products['shortName']:
                            products['Portion Size'] = "Standard"
                        elif "HF" in products['shortName']:
                            products['Portion Size'] = "Half"
                        else:
                            products['Portion Size'] = str(products['shortName']).split()[-1]

                        products = helpers.delete_items(products, del_p_list)
                        products = helpers.rename_items(products, alter_p_list)
                        products = helpers.keep_items(products, price_headers)

                        Data.append(products)

                        yield products
                except:
                    pass

        self.out = self.out.append(pd.json_normalize(Data))

        Times=json_response['oat']['openingTimes']

        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

        for i in days:
            if Times[i]['open'] is True:
                Times[i]
                OT = {}
                OT['Site Number'] = f'''MST-{json_response['_id']}'''
                OT['Date'] = Date
                OT['Day'] = i.capitalize()
                OT['Open'] = f'''{str(Times[i]['from']['hour']).zfill(2)}:{str(Times[i]['from']['minute']).zfill(2)}'''
                OT['Close'] = f'''{str(Times[i]['to']['hour']).zfill(2)}:{str(Times[i]['to']['minute']).zfill(2)}'''
                yield OT
                self.ot = self.ot.append(pd.json_normalize(OT))

    def closed(self, reason):
        if self.out is not None:
            out = self.out[variables.price_headers]
            print(self.out.head(10).to_string())
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                       dtype=variables.price_dtype_C)

        if self.ot is not None:
            print(self.ot.head(100).to_string())
            ot = self.ot[['Site Number', 'Date', 'Day', 'Open', 'Close']]
            database_Output = 'COMPETITOR_OPENING_TIMES'
            ot.to_sql(name=database_Output, con=engine, if_exists='append', index=False, chunksize=16000)


class MarstonsOT(scrapy.Spider):
    name = 'MST-Opening-Times'
    down = 'marstons-sites-app'
    out = pd.DataFrame()

    allowed_domains=allowed_domains
    start_urls=[m_url]

    def start_requests(self):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))

        for site in job.items.iter(count=1):
            slug=site['slug']
            urlS = "https://api-cdn.orderbee.co.uk/venues/{0}".format(slug)

            pubName=site['name']
            pubCode=site['_id']

            time.sleep(random.randint(0,2))
            yield Request(urlS,
                            method="GET",
                            headers=variables.m_headers,
                            callback=self.parse_detail,
                            meta={'pubName':pubName,'pubCode':pubCode})

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        Times=json_response['oat']['openingTimes']

        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

        for i in days:
            if Times[i]['open'] is True:
                Times[i]
                OT = {}
                OT['Site Number'] = f'''MST-{json_response['_id']}'''
                OT['Date'] = Date
                OT['Day'] = i.capitalize()
                OT['Open'] = f'''{str(Times[i]['from']['hour']).zfill(2)}:{str(Times[i]['from']['minute']).zfill(2)}'''
                OT['Close'] = f'''{str(Times[i]['to']['hour']).zfill(2)}:{str(Times[i]['to']['minute']).zfill(2)}'''
                yield OT
                self.out = self.out.append(pd.json_normalize(OT))


    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(100).to_string())
            out = self.out[['Site Number', 'Date', 'Day', 'Open', 'Close']]
            database_Output = 'COMPETITOR_OPENING_TIMES'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000)
