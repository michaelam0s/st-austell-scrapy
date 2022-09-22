# -*- coding: utf-8 -*-
import re
import io
import json
import time
import numpy as np
import scrapy
import random
import datetime
from datetime import datetime
import pandas as pd
import hashlib

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request
import logging
logging.getLogger("snowflake.connector.network").disabled = True

Testmode = True
N = 3 if Testmode else 10**8

Date = datetime.today().strftime('%Y-%m-%d')

url = variables.w_url
allowed_domains = variables.w_allowed_domains

class StarPubsSitesTest(scrapy.Spider):
    name = 'StarPubs-Sites'

    def start_requests(self):
        url = 'https://iopapi.zonalconnect.com/'
        Payload = 'request=' + urllib.parse.quote(json.dumps(variables.SP_Site_Payload)).replace(" ", "")

        yield scrapy.Request(url,
                     method='POST',
                     headers=variables.SP_headers,
                     body=Payload,
                     callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.text)
        sites = json_response['venues']

        for site in sites:
            yield site

class StarPubsSitesFormatted(scrapy.Spider):
    name = 'StarPubs-Sites-Formatted'

    out = pd.DataFrame()

    def start_requests(self):
        url = 'https://iopapi.zonalconnect.com/'
        Payload = 'request=' + urllib.parse.quote(json.dumps(variables.SP_Site_Payload)).replace(" ", "")
        payload = "request=%7B%22request%22%3A%20%7B%22username%22%3A%20%22star-pubs-wla%22%2C%20%22password%22%3A%20%226x8a6gqj%22%2C%20%22version%22%3A%20%221.0.0%22%2C%20%22bundleIdentifier%22%3A%20%22com.heinekenstarorder%22%2C%20%22platform%22%3A%20%22Android%22%2C%20%22userDeviceIdentifier%22%3A%20%2229884031-88a4-434c-88a4-216a8f1e7cb4%22%2C%20%22method%22%3A%20%22venues%22%7D%7D"

        yield scrapy.Request(url,
                     method='POST',
                     headers=variables.SP_headers,
                     body=payload,
                     callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.text)
        sites = json_response['venues']
        Data = []
        for site in sites:
            site['Address Line 1'] = site['address']['line1']
            site['Format'] = 'Star Pubs'
            site['Company'] = 'Star Pubs'
            site['Country'] = site['address']['country']['name']
            site['County'] = site['address']['county']
            site['Date'] = Date
            site['Latitude'] = site['address']['location']['latitude']
            site['Longitude'] = site['address']['location']['longitude']
            site['Other Town'] = site['address']['town']
            site['Postcode'] = site['address']['postcode']
            site['Site Number'] = f'''SP-{site['id']}'''
            site['Status'] = 'OPEN'
            site['Given ID'] = site['id']
            site['Site Name'] = site['name']
            site['Telephone Number'] = 0
            site['Town'] = site['address']['town']

            site = helpers.keep_items(site, variables.site_headers)
            Data.append(site)

            yield site

        self.out = self.out.append(pd.json_normalize(Data))

    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(10).to_string())
            out = self.out[variables.site_headers]
            out = out.replace(r'^\s*$', np.nan, regex=True)
            database_Output = 'comepetitor_sites'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                       dtype=variables.price_dtype)

class StarPubsMenus(scrapy.Spider):
    name = 'StarPubs-Menus'
    down = 'StarPubs-Sites'

    def start_requests(self):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))
        url = 'https://iopapi.zonalconnect.com/'

        for site in job.items.iter(count=N):
            name = site['name']
            siteId = site['id']
            try:
                salesAreaId = site['salesArea'][0]['id']
            except:
                salesAreaId = 0

            variables.SP_Site_Payload['request']['salesAreaId'] = salesAreaId
            variables.SP_Site_Payload['request']['serviceId'] = 1
            variables.SP_Site_Payload['request']['venueId'] = siteId
            variables.SP_Site_Payload['request']['siteId'] = siteId
            variables.SP_Site_Payload['request']['method'] = "getMenus"

            menu_data = urllib.parse.quote(json.dumps(variables.SP_Site_Payload))
            bdy = "request=" + menu_data

            time.sleep(random.randint(0, 2))
            yield scrapy.Request(url,
                          method='POST',
                          body=bdy,
                          headers=variables.SP_Menu_headers,
                          callback=self.parse_detail,
                          meta = {'Site Name': name, 'Site Number': siteId})

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        menus=json_response['menus']

        for menu in menus:
            menu['Menu Name'] = menu['name']
            menu['Site Name'] = response.meta['Site Name']
            menu['Site Number'] = response.meta['Site Number']
            yield menu

class StarPubsPrices(scrapy.Spider):
    name = 'StarPubs-Prices-Formatted'
    down = 'StarPubs-Menus'

    out = pd.DataFrame()
    def start_requests(self):
        url = 'https://iopapi.zonalconnect.com/'

        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))

        for menu in job.items.iter(count=N):
            variables.SP_Site_Payload['request']['method'] = "getmenupages"
            variables.SP_Site_Payload['request']['siteId'] = menu['Site Number']
            variables.SP_Site_Payload['request']['salesAreaId'] = menu['salesAreaId']
            variables.SP_Site_Payload['request']['menuId'] = menu['id']

            menu_data = urllib.parse.quote(json.dumps(variables.SP_Site_Payload))
            bdy = "request=" + menu_data

            yield scrapy.Request(url,
                          method='POST',
                          body=bdy,
                          headers=variables.SP_headers,
                          callback=self.parse_detail,
                          meta={'Site Name': menu['Site Name'], 'Site Number': menu['Site Number'], 'salesAreaId': menu['salesAreaId'], 'Menu ID': menu['id'],
                                'Menu Name': menu['name']})

            time.sleep(random.randint(0, 2))

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        prices = json_response['aztec']['products']

        Data = []
        for price in prices:
            prod = price['eposName']
            for products in price['portions']:
                products['Site Name'] = response.meta['Site Name']
                products['Site Number'] = f'''SP-{response.meta['Site Number']}'''
                products['Menu ID'] = response.meta['Menu ID']
                products['Menu Name'] = response.meta['Menu Name']
                products['Product Name'] = price['eposName']
                Prod_Name = price['eposName']
                Prod_ID = str(price['eposName']) + str(products['name'])
                products['Product ID'] = hashlib.md5(Prod_ID.encode()).hexdigest()
                products['Date'] = Date
                products['Company'] = 'Star Pubs'
                products['Format'] = 'Star Pubs'
                products['Portion Size'] = products['name']
                try:
                    products['Price'] = products['price']
                except:
                    products['Price'] = 0
                products = helpers.keep_items(products, variables.price_headers)

                Data.append(products)

                yield products

        self.out = self.out.append(pd.json_normalize(Data))

    def closed(self, reason):
        if self.out is not None:
            out = self.out[variables.price_headers]
            print(self.out.head(10).to_string())
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                       dtype=variables.price_dtype_C)