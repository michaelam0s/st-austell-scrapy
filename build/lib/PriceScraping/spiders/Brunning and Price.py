# -*- coding: utf-8 -*-
import re
import io
import json
import time
import numpy as np

import scrapy
import random
from datetime import datetime
import hashlib
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from scrapy import linkextractors

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request
import logging
logging.getLogger("snowflake.connector.network").disabled = True

Testmode = False
Limit = True

N = 3 if Testmode else 10**8

Date = datetime.today().strftime('%Y-%m-%d')

class BandPSites(scrapy.Spider):
    name = 'bnp-sites-formatted'

    site_out = []

    i = 0

    def start_requests(self):
        url = 'https://www.brunningandprice.co.uk/company/pub-contacts/'

        yield Request(
            url,
            method='GET',
            callback=self.parse
        )
    def parse(self, response):
        resp = response
        print(resp.text)

        for sites in resp.xpath('/html/body/main/section[2]/div/div/div/div/div/article'):
            # try:
            site = {}
            site['Address Line 1'] = sites.xpath('div[1]/div[2]/div/div[1]/p[1]/text()[2]').extract()[0]
            site['Company'] = "Brunning and Price"
            site['Country'] = "UK"
            site['County'] = sites.xpath('div[1]/div[2]/div/div[1]/p[1]/text()[last()-2]').extract()[0]
            site['Date'] = Date
            site['Format'] = "Brunning and Price"
            site['Given ID'] = sites.xpath('@id').extract()[0]
            site['Postcode'] = str(sites.xpath('div[1]/div[2]/div/div[1]/p[1]/text()[last()]').extract()[0]).replace("%20", " ")
            site['Site Name'] = sites.xpath('h3/text()[normalize-space()]').extract()[0]
            site['Site Number'] = sites.xpath('@id').extract()[0]
            site['Status'] = 'OPEN'
            site['Telephone Number'] = ""
            print(site['Postcode'])

            yield Request(
                url=f'''https://api.postcodes.io/postcodes/{site['Postcode']}''',
                method='GET',
                callback=self.parse_locations,
                meta=site
            )
            print("-----------")
            # except:
            #     pass

    def parse_locations(self, response):
        site = response.meta
        json_response = json.loads(response.text)
        site['Address Line 1'] = response.meta['Address Line 1']
        site['Company'] = response.meta['Company']
        site['Country'] = response.meta['Country']
        site['County'] = response.meta['County']
        site['Date'] = response.meta['Date']
        site['Format'] = response.meta['Format']
        site['Given ID'] = response.meta['Given ID']
        site['Latitude'] = json_response['result']['latitude']
        site['Longitude'] = json_response['result']['longitude']
        site['Other Town'] = json_response['result']['admin_district']
        site['Postcode'] = response.meta['Postcode']
        site['Site Name'] = response.meta['Site Name']
        site['Site Number'] = f'''BP-{response.meta['Site Number']}'''
        site['Status'] = response.meta['Status']
        site['Telephone Number'] = response.meta['Telephone Number']
        site['Town'] = json_response['result']['admin_district']

        self.site_out.append(site)
        yield site

    def closed(self, reason):
        Site_Data = pd.json_normalize(self.site_out)
        Site_Data = Site_Data[variables.site_headers]
        Site_Data = Site_Data.astype(str)
        print(Site_Data.head(5).to_string())
        Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)
        if Site_Data is not None:
            database_Output = 'competitor_sites'
            Site_Data.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000, dtype=variables.site_dtype)
        else:
            print("Empty Dataframe")
            pass



class BandPPrices(scrapy.Spider):
    name = 'bnp-prices-formatted'
    i = 0
    out = pd.DataFrame()

    def start_requests(self):
        url = 'https://www.brunningandprice.co.uk/company/pub-contacts/'

        yield Request(
            url,
            method='GET',
            callback=self.parse
        )
    def parse(self, response):
        resp = response

        for site in resp.xpath('/html/body/main/section[2]/div/div/div/div/div/article'):
            if self.i <=N:
                Site_Name = site.xpath('h3/text()[normalize-space()]').extract()[0]
                Site_Number = site.xpath('@id').extract()[0]
                Site_Website_S = site.xpath('div/div')[0]
                url = Site_Website_S.xpath('a/@href').extract()[0]


                yield Request(
                    url,
                    method='GET',
                    callback=self.site_websites,
                    meta={
                        "Site Name": Site_Name,
                        "Site Number": Site_Number,
                        "Website": url
                    }
                )
                self.i = self.i + 1
                time.sleep(random.randint(1, 3))

    def site_websites(self, response):
        for menu in response.xpath('/html/body/nav/div/div/ul/li[2]/div/a'):
            Menu_URL = menu.xpath('@href').extract()[0]


            url = f'''https://www.brunningandprice.co.uk/{Menu_URL}'''
            response.meta['Menu URL'] = url

            yield Request(
                url,
                method='GET',
                callback=self.menus,
                meta=response.meta
            )
            time.sleep(random.randint(1, 3))
    def menus(self, response):

        Data = []

        Menu = response.xpath('/html/body/main/section/div/div/div/div/div/div[1]/h2/text()').extract()[0]
        for items in response.xpath('/html/body/main/section/div/div/div/div/div/div[1]/div[2]/div'):
            for products in items.xpath('p'):
                try:
                    Product = products.xpath('span[1]/text()').extract()[0]
                    Price = ""
                    if any(char.isdigit() for char in products.xpath('span[3]/text()').extract()[0]):
                        Price = products.xpath('span[3]/text()').extract()[0]
                    elif any(char.isdigit() for char in products.xpath('span[2]/text()').extract()[0]):
                        Price = products.xpath('span[2]/text()').extract()[0]

                    if Price is not None and Price != "" and Price != " " and any(char.isdigit() for char in Price) is True:
                        Price = helpers.Price_Select(Price)
                        Products = {}
                        Products['Company'] = "Brunning and Price"
                        Products['Date'] = Date
                        Products['Format'] = "Brunning and Price"
                        Products['Menu ID'] = hashlib.md5(Menu.encode()).hexdigest()
                        Products['Menu Name'] = Menu
                        Products['Portion Size'] = "Standard"
                        Products['Price'] = Price
                        Products['Product ID'] = hashlib.md5(Product.encode()).hexdigest()
                        Products['Product Name'] = Product
                        Products['Site Name'] = response.meta['Site Name']
                        Products['Site Number'] = f'''BP-{response.meta['Site Number']}'''

                        try:
                            float(Price)
                            Data.append(Products)
                            yield Products

                        except:
                            pass
                except:
                    pass

        try:
            self.out = self.out.append(pd.json_normalize(Data))
        except:
            pass

        if len(self.out.index) >= 100000:
            out = self.out[variables.price_headers]
            print(self.out.head(10).to_string())
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                       dtype=variables.price_dtype_C)
            self.out = pd.DataFrame()

    def closed(self, reason):
        if self.out is not None:
            out = self.out[variables.price_headers]
            print(self.out.head(10).to_string())
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                       dtype=variables.price_dtype_C)
