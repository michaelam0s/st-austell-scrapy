# -*- coding: utf-8 -*-
import re
import io
import json
import time
import pandas as pd
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

s_url=variables.r_urlS
m_url=variables.r_urlM
allowed_domains=variables.r_allowed_domains

Testmode = False
N = 1 if Testmode else 10**8

Date = datetime.today().strftime('%Y-%m-%d')

class RevsSiteSpider(scrapy.Spider):
    name='revs-sites'

    allowed_domains=allowed_domains
    start_urls=[s_url]

    def start_requests(self):
        for u in self.start_urls:
            u=u+"locations?per_page=999&summary=1"+"&services=order_to_table"
            yield Request(u,
                            method="GET",
                            headers=variables.r_headers,
                            callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        venues=json_response['locations']

        for venue in venues:
            yield venue

class RevsSiteSpiderFormatted(scrapy.Spider):
    name = 'revs-sites-formatted'

    allowed_domains=allowed_domains
    start_urls=[s_url]

    out = pd.DataFrame()

    def start_requests(self):
        for u in self.start_urls:
            u=u+"locations?per_page=999&summary=1"+"&services=order_to_table"
            yield Request(u,
                            method="GET",
                            headers=variables.r_headers,
                            callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        venues=json_response['locations']

        for venue in venues[:N]:
            url = f'''https://api.pepperhq.com/locations/{venue['_id']}'''
            yield scrapy.Request(url,
                                 method='GET',
                                 headers=variables.r_headers,
                                 callback=self.parse)
            time.sleep(random.randint(0, 2))

    def parse(self, response):
        json_response = json.loads(response.text)
        site = json_response

        site['Address Line 1'] = site['address']['address']
        site['Format'] = 'Revolution Bars'
        site['Company'] = 'Revolution Bars'
        site['Country'] = site['address']['country']
        site['Date'] = Date
        site['Latitude'] = site['geo'][1]
        site['Longitude'] =site['geo'][0]
        site['Town'] = site['address']['town']
        site['Other Town'] = site['address']['town']
        site['Postcode'] = site['address']['postcode']
        site['Site Number'] = f'''RVS-{site['_id']}'''
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

class RevsPriceSpider(scrapy.Spider):
    name='revs-prices-formatted'
    down = 'revs-sites-formatted'
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
                                 headers=variables.r_headers,
                                 callback=self.parse_detail,
                                 meta={'Site Name': site['Site Name'],
                                       'Site Number': site['Site Number']})
            time.sleep(random.randint(0, 2))

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        prices = json_response['categories']

        for menus in prices:
            for products in menus['products']:
                products['Site Name'] = response.meta['Site Name']
                products['Site Number'] = response.meta['Site Number']
                products['Company'] = 'Revolution Bars'
                products['Date'] = Date
                products['Format'] = 'Revolution Bars'
                products['Menu ID'] = menus['id']
                products['Menu Name'] = re.sub(r'\W+', '', menus['title'])
                products['Price'] = products['price']
                products['Product ID'] = products['id']

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
            database_Output = 'competitor_prices'
            Price_Data.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                       dtype=variables.price_dtype_C)


class RevsOpeningTimes(scrapy.Spider):
    name = 'RVS-Opening-Times'

    allowed_domains=allowed_domains
    start_urls=[s_url]

    out = pd.DataFrame()

    def start_requests(self):
        for u in self.start_urls:
            u=u+"locations?per_page=999&summary=1"+"&services=order_to_table"
            yield Request(u,
                            method="GET",
                            headers=variables.r_headers,
                            callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        venues=json_response['locations']

        for venue in venues[:N]:
            url = f'''https://api.pepperhq.com/locations/{venue['_id']}'''
            yield scrapy.Request(url,
                                 method='GET',
                                 headers=variables.r_headers,
                                 callback=self.parse)
            time.sleep(random.randint(0, 2))

    def parse(self, response):
        json_response = json.loads(response.text)
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        print(json_response['openingHours'])
        for day in days:
            OT = {}
            OT['Site Number'] = f'''RVS-{json_response['_id']}'''
            OT['Date'] = Date
            OT['Day'] = day.capitalize()
            if json_response['openingHours'][day] == 'closed':
                OT['Open'] = 'CLOSED'
                OT['Close'] = 'CLOSED'
            else:
                OT['Open'] = json_response['openingHours'][day].split(" - ")[0].replace('pm', '').replace('am', '')
                OT['Close'] = json_response['openingHours'][day].split(" - ")[1].replace('pm', '').replace('am', '')

            yield OT
            self.out = self.out.append(pd.json_normalize(OT))

    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(100).to_string())
            out = self.out[['Site Number', 'Date', 'Day', 'Open', 'Close']]
            database_Output = 'COMPETITOR_OPENING_TIMES'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000)