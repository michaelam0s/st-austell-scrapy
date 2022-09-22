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

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request
import logging
logging.getLogger("snowflake.connector.network").disabled = True

url = variables.w_url
allowed_domains = variables.w_allowed_domains

del_list = variables.w_del_list
alter_list = variables.w_alter_list
s_keep_list = variables.w_s_keep_list
s_alter_list = variables.w_s_alter_list

site_dtype = variables.site_dtype
site_headers = variables.site_headers

price_headers = variables.price_headers
price_dtype = variables.price_dtype

engine = variables.engine

Limit = False
TestMode = False
N = 3
Date = datetime.today().strftime('%Y-%m-%d')
Company = 'JDW'


class WetherspoonsSiteSpider(scrapy.Spider):
    name = 'wetherspoons-sites-formatted'

    allowed_domains = variables.w_allowed_domains
    start_urls = [variables.w_url]
    site_out = []

    def parse(self, response):
        resp = json.loads(response.body)
        venues = resp.get('venues')

        for site in venues:
            site['Site Number'] = f'''JDW-{site['iOrderId']}'''
            site['Status'] = 'OPEN'
            if site['pubIsClosed'] == 'true':
                site['Status'] = 'CLOSED'
            site = helpers.keep_items(site, s_keep_list)
            site = helpers.rename_items(site, s_alter_list)
            site['Format'] = 'JD Wetherspoon'
            site['Company'] = 'JD Wetherspoon'
            site['Date'] = Date
            site['Other Town'] = site['Town']
            site['Telephone Number'] = 'N/A'

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
            Site_Data.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                             dtype=site_dtype)
        else:
            print("Empty Dataframe")
            pass


class WetherspoonsPriceSpider(scrapy.Spider):
    name = 'wetherspoons-prices-formatted'
    start_urls = [variables.w_url]
    out = pd.DataFrame()

    def parse(self, response):
        resp = json.loads(response.body)
        sites = resp['venues']

        if TestMode is True:
            sites = sites[5:12]

        for site in sites:
            Site_ID = site['venueId']
            Site_Name = site['name']
            iOrder_ID = site['iOrderId']
            time.sleep(random.randint(0, 2))
            temp_url = f'https://static.wsstack.nn4maws.net/content/v4/menus/{Site_ID}.json'
            response = Request(url=temp_url, callback=self.parse_detail,
                               meta={'Venue_Name': Site_Name, 'Site_ID': Site_ID, 'iOrder_ID': iOrder_ID})
            yield response

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        test = json_response['menus']

        # if TestMode is True:
        #     test = test[0:5]

        Data = []
        for items in test:
            Menu = items['name']
            for sm in items['subMenu']:
                for sm2 in sm['productGroups']:
                    for products in sm2['products']:
                        if not products['portions']:
                            product = {}
                            product['Company'] = 'JD Wetherspoon'
                            product['Date'] = Date
                            product['Format'] = 'JD Wetherspoon'
                            product['Menu ID'] = items['menuId']
                            product['Menu Name'] = Menu
                            product['Portion Size'] = products['defaultPortionName']
                            product['Price'] = products['priceValue']
                            product['Product ID'] = products['productId']
                            product['Product Name'] = products['eposName']
                            product['Site Name'] = response.meta['Venue_Name']
                            product['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
                            product['Calories'] = products['calories']

                            try:
                                # if portions['showPortion'] is True:
                                product = helpers.keep_items(product, variables.price_headers_C)

                                Data.append(product)

                                yield product
                            except:
                                pass

                        else:
                            for portions in products['portions']:
                                product = {}
                                product['Company'] = 'JD Wetherspoon'
                                product['Date'] = Date
                                product['Format'] = 'JD Wetherspoon'
                                product['Menu ID'] = items['menuId']
                                product['Menu Name'] = Menu
                                product['Portion Size'] = portions['name']
                                product['Price'] = portions['price']
                                product['Product ID'] = str(products['productId']) + "-" + str(portions['id'])
                                product['Product Name'] = products['eposName']
                                product['Site Name'] = response.meta['Venue_Name']
                                product['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
                                product['Calories'] = products['calories']

                                if portions['showPortion'] is True:
                                    product = helpers.keep_items(product, variables.price_headers_C)

                                    Data.append(product)

                                    yield product

        self.out = self.out.append(pd.json_normalize(Data))
        if len(self.out.index) >= 100000:
            out = self.out[variables.price_headers_C]
            out = out.astype(str)
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                       chunksize=16000,
                       dtype=variables.price_dtype_C)
            self.out = pd.DataFrame()

    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(10).to_string())
            out = self.out[variables.price_headers_C]
            out = out.astype(str)
            out = out.replace(r'^\s*$', np.nan, regex=True)
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                       chunksize=16000,
                       dtype=variables.price_dtype_C)


class WetherspoonsPriceSpiderDaily(scrapy.Spider):
    name = 'wetherspoons-prices-formatted-daily'
    start_urls = [variables.w_url]
    out = pd.DataFrame()

    def parse(self, response):
        resp = json.loads(response.body)
        sites = resp['venues']
        sites = random.sample(sites, 20)

        for site in sites:
            Site_ID = site['venueId']
            Site_Name = site['name']
            iOrder_ID = site['iOrderId']
            time.sleep(random.randint(0, 10))
            temp_url = f'https://static.wsstack.nn4maws.net/content/v4/menus/{Site_ID}.json'
            response = Request(url=temp_url, callback=self.parse_detail,
                               meta={'Venue_Name': Site_Name, 'Site_ID': Site_ID, 'iOrder_ID': iOrder_ID})
            yield response

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        test = json_response['menus']

        # if TestMode is True:
        #     test = test[0:5]

        Data = []
        for items in test:
            Menu = items['name']
            for sm in items['subMenu']:
                for sm2 in sm['productGroups']:
                    for products in sm2['products']:
                        if not products['portions']:
                            product = {}
                            product['Company'] = 'JD Wetherspoon'
                            product['Date'] = Date
                            product['Format'] = 'JD Wetherspoon'
                            product['Menu ID'] = items['menuId']
                            product['Menu Name'] = Menu
                            product['Portion Size'] = products['defaultPortionName']
                            product['Price'] = products['priceValue']
                            product['Product ID'] = products['productId']
                            product['Product Name'] = products['eposName']
                            product['Site Name'] = response.meta['Venue_Name']
                            product['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
                            product['Calories'] = products['calories']

                            try:
                                # if portions['showPortion'] is True:
                                product = helpers.keep_items(product, variables.price_headers_C)

                                Data.append(product)

                                yield product
                            except:
                                pass

                        else:
                            for portions in products['portions']:
                                product = {}
                                product['Company'] = 'JD Wetherspoon'
                                product['Date'] = Date
                                product['Format'] = 'JD Wetherspoon'
                                product['Menu ID'] = items['menuId']
                                product['Menu Name'] = Menu
                                product['Portion Size'] = portions['name']
                                product['Price'] = portions['price']
                                product['Product ID'] = str(products['productId']) + "-" + str(portions['id'])
                                product['Product Name'] = products['eposName']
                                product['Site Name'] = response.meta['Venue_Name']
                                product['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
                                product['Calories'] = products['calories']

                                if portions['showPortion'] is True:
                                    product = helpers.keep_items(product, variables.price_headers_C)

                                    Data.append(product)

                                    yield product

        self.out = self.out.append(pd.json_normalize(Data))
        if len(self.out.index) >= 100000:
            out = self.out[variables.price_headers_C]
            out = out.astype(str)
            database_Output = 'competitor_price_tracking'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                       chunksize=16000,
                       dtype=variables.price_dtype_C)
            self.out = pd.DataFrame()

    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(10).to_string())
            out = self.out[variables.price_headers_C]
            out = out.astype(str)
            out = out.replace(r'^\s*$', np.nan, regex=True)
            database_Output = 'competitor_price_tracking'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                       chunksize=16000,
                       dtype=variables.price_dtype_C)


class WetherspoonsPriceUpdatedSpider(scrapy.Spider):
    name = 'T'
    start_urls = [variables.w_url]
    out = pd.DataFrame()

    def parse(self, response):
        resp = json.loads(response.body)
        sites = resp['venues']

        print(len(sites))

        R_Sample = random.sample(range(5, len(sites)), 2)
        print(R_Sample)

        for site in R_Sample:
            Site_ID = sites[site]['venueId']
            Site_Name = sites[site]['name']
            iOrder_ID = sites[site]['iOrderId']
            time.sleep(random.randint(0, 2))
            temp_url = f'https://static.wsstack.nn4maws.net/content/v4/menus/{Site_ID}.json'
            response = Request(url=temp_url, callback=self.parse_detail,
                               meta={'Venue_Name': Site_Name, 'Site_ID': Site_ID, 'iOrder_ID': iOrder_ID})
            yield response

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        menus = json_response['menus']
        # print(test)
        Data = []

        for sm in menus:
            for pg in sm['subMenu']:
                for products in pg['productGroups']:
                    for product in products['products']:
                        if not product['portions']:
                            prod = {}
                            prod['Product ID'] = product['productId']
                            prod['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
                            prod['Price'] = product['priceValue']
                            Data.append(prod)
                        else:
                            for portions in product['portions']:
                                prod = {}
                                prod['Product ID'] = str(product['productId']) + "-" + str(portions['id'])
                                prod['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
                                prod['Price'] = portions['price']
                                Data.append(prod)

        R_Sample = random.sample(range(0, len(Data)), 20)
        Prod_L = f'''('{"', '".join(str(x['Product ID']) for x in [Data[x] for x in R_Sample])}')'''
        print(Prod_L)
        query = f''' select * from  '''


    #
    #     if TestMode is True:
    #         test = test[0:N]
    #
    #     Data = []
    #     for items in test:
    #         Menu = items['name']
    #         for sm in items['subMenu']:
    #             for sm2 in sm['productGroups']:
    #                 for products in sm2['products']:
    #                     if not products['portions']:
    #                         product = {}
    #                         product['Company'] = 'JD Wetherspoon'
    #                         product['Date'] = Date
    #                         product['Format'] = 'JD Wetherspoon'
    #                         product['Menu ID'] = items['menuId']
    #                         product['Menu Name'] = Menu
    #                         product['Portion Size'] = products['defaultPortionName']
    #                         product['Price'] = products['priceValue']
    #                         product['Product ID'] = products['productId']
    #                         product['Product Name'] = products['eposName']
    #                         product['Site Name'] = response.meta['Venue_Name']
    #                         product['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
    #                         product['Calories'] = products['calories']
    #
    #                         try:
    #                             if portions['showPortion'] is True:
    #                                 product = helpers.keep_items(product, variables.price_headers_C)
    #
    #                                 Data.append(product)
    #
    #                                 yield product
    #                         except:
    #                             pass
    #
    #                     else:
    #                         for portions in products['portions']:
    #                             product = {}
    #                             product['Company'] = 'JD Wetherspoon'
    #                             product['Date'] = Date
    #                             product['Format'] = 'JD Wetherspoon'
    #                             product['Menu ID'] = items['menuId']
    #                             product['Menu Name'] = Menu
    #                             product['Portion Size'] = portions['name']
    #                             product['Price'] = portions['price']
    #                             product['Product ID'] = str(products['productId']) + "-" + str(portions['id'])
    #                             product['Product Name'] = products['eposName']
    #                             product['Site Name'] = response.meta['Venue_Name']
    #                             product['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
    #                             product['Calories'] = products['calories']
    #
    #                             if portions['showPortion'] is True:
    #                                 product = helpers.keep_items(product, variables.price_headers_C)
    #
    #                                 Data.append(product)
    #
    #                                 yield product
    #
    #     self.out = self.out.append(pd.json_normalize(Data))
    #     if len(self.out.index) >= 100000:
    #         out = self.out[variables.price_headers_C]
    #         out = out.astype(str)
    #         database_Output = 'ingestion_competitor_prices'
    #         out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
    #                    chunksize=16000,
    #                    dtype=variables.price_dtype_C)
    #         self.out = pd.DataFrame()
    #
    # def closed(self, reason):
    #     if self.out is not None:
    #         print(self.out.head(10).to_string())
    #         out = self.out[variables.price_headers_C]
    #         out = out.astype(str)
    #         out = out.replace(r'^\s*$', np.nan, regex=True)
    #         database_Output = 'ingestion_competitor_prices'
    #         out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
    #                    chunksize=16000,
    #                    dtype=variables.price_dtype_C)

# class WetherspoonsPriceSpiderLimitedKeyProducts(scrapy.Spider):
#     name = 'wetherspoons-prices-formatted-key-products'
#     start_urls = [variables.w_url]
#     data_out = []
#
#     def parse(self, response):
#         resp = json.loads(response.body)
#         sites = resp['venues']
#
#         if TestMode is True:
#             sites = sites[0:N]
#         print(sites)
#         for site in sites:
#             Site_ID = site['venueId']
#             Site_Name = site['name']
#             iOrder_ID = site['iOrderId']
#             time.sleep(random.randint(0, 2))
#             temp_url = f'https://static.wsstack.nn4maws.net/content/v4/menus/{Site_ID}.json'
#             response = Request(url=temp_url, callback=self.parse_detail,
#                                meta={'Venue_Name': Site_Name, 'Site_ID': Site_ID, 'iOrder_ID': iOrder_ID})
#             yield response
#
#     def parse_detail(self, response):
#         json_response = json.loads(response.text)
#         test = json_response['menus']
#
#         if TestMode is True:
#             test[0:N]
#
#         for items in test:
#             Menu = items['name']
#             for sm in items['subMenu']:
#                 for sm2 in sm['productGroups']:
#                     for products in sm2['products']:
#                         products['Date'] = Date
#                         products['Menu Name'] = Menu
#                         products['Site Name'] = response.meta['Venue_Name']
#                         products['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
#                         products['Company'] = 'JD Wetherspoons'
#                         products['Format'] = 'Pub'
#
#                         products = helpers.delete_items(products, del_list)
#                         products = helpers.rename_items(products, alter_list)
#                         products = helpers.keep_items(products, price_headers)
#
#                         if str(products['Product ID']) in y:
#                             print(products['Product ID'])
#                             print(products['Product Name'].lower())
#                             products['Stonegate Product'] = helpers.Key_Products_To_Json(Key_Products_Filtered,
#                                                                                          products['Product ID'])
#                             self.data_out.append(products)
#                             yield products



class WetherspoonsTesting(scrapy.Spider):
    name = 'wetherspoons-testing'
    start_urls = [variables.w_url]
    out = pd.DataFrame()

    def parse(self, response):
        resp = json.loads(response.body)
        sites = resp['venues']

        for site in sites:
            Site_ID = site['venueId']
            Site_Name = site['name']
            iOrder_ID = site['iOrderId']
            time.sleep(random.randint(0, 1))
            temp_url = f'https://static.wsstack.nn4maws.net/content/v4/menus/{Site_ID}.json'
            response = Request(url=temp_url, callback=self.parse_detail,
                               meta={'Venue_Name': Site_Name, 'Site_ID': Site_ID, 'iOrder_ID': iOrder_ID})
            yield response

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        test = json_response['menus']

        Data = []
        for items in test:
            Menu = items['name']

            for sm in items['subMenu']:
                for sm2 in sm['productGroups']:
                    for products in sm2['products']:
                        products['Date'] = Date
                        products['Menu Name'] = Menu
                        products['Site Name'] = response.meta['Venue_Name']
                        products['Site Number'] = f'''JDW-{response.meta['iOrder_ID']}'''
                        products['Company'] = 'JDW'
                        products['Format'] = 'Pub'

                        products = helpers.delete_items(products, del_list)
                        products = helpers.rename_items(products, alter_list)
                        products = helpers.keep_items(products, price_headers)

                        Data.append(products)

                        yield products
        self.out = self.out.append(pd.json_normalize(Data))


    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(10).to_string())
            out = self.out[price_headers]
            database_Output = 'Competitor_Pricing_Test_2'
            out.to_sql(name=database_Output, con=engine, if_exists='append', index=False, chunksize=16000,
                              dtype=price_dtype)


class JDWOpeningTimes(scrapy.Spider):
    name = 'JDW-Opening-Times'

    out = pd.DataFrame()

    def start_requests(self):
        url = 'https://static.wsstack.nn4maws.net/v1/venues/en_gb/venues.json'

        yield scrapy.Request(url,
                             method='GET',
                             callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.text)

        json_response = json_response['venues']

        for site in json_response:
            yield scrapy.Request(f'''https://static.wsstack.nn4maws.net/v1/venues/en_gb/{site['venueId']}.json''',
                                 method='GET',
                                 callback=self.parse_sites)

            time.sleep(random.randint(0, 1))

    def parse_sites(self, response):
        json_response = json.loads(response.text)

        for days in json_response['openingTimes']:
            OT = {}
            days['Open'] = days['times'].split(" - ")[0]
            days['Close'] = days['times'].split(" - ")[1]
            days['Site Number'] = f'''JDW-{json_response['iOrderId']}'''
            OT['Site Number'] = f'''JDW-{json_response['iOrderId']}'''
            OT['Date'] = Date
            OT['Day'] = days['label']
            O = days['times'].split(" - ")[0]
            C = days['times'].split(" - ")[1]
            if "pm" in O:
                O = O.replace("pm","")
                O = f'''{str(int(O.split(":")[0])+12)}:{O.split(":")[1]}'''
            else:
                O = O.replace("am", "")
                O = f'''{O.split(":")[0]}:{O.split(":")[1]}'''

            if "pm" in C:
                print(C)
                C = C.replace("pm","")

                if int(C.split(":")[0]) == 0:
                    C = f'''{str(int(C.split(":")[0]))}:{C.split(":")[1]}'''
                else:
                    C = f'''{str(int(C.split(":")[0])+12)}:{C.split(":")[1]}'''
            else:
                print(C)
                C = C.replace("am", "")
                if int(C.split(":")[0]) == 12:
                    C = f'''00:{C.split(":")[1]}'''
                else:
                    C = f'''{C.split(":")[0]}:{C.split(":")[1]}'''

            OT['Open'] = O
            OT['Close'] = C
            yield OT
            self.out = self.out.append(pd.json_normalize(OT))

    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(100).to_string())
            out = self.out[['Site Number', 'Date', 'Day', 'Open', 'Close']]
            database_Output = 'COMPETITOR_OPENING_TIMES'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000)


class JDWPriceCheck(scrapy.Spider):
    name = 'JDW-Price-Check'


    def start_request(self):
        engine = variables.engine

        query = ''' select * from "PROD_WAREHOUSE"."TEMP"."Competitor_Pricing_All_Products" where "Company" = 'JD Wetherspoon' order by random() limit 1 '''
        Key_Products = pd.read_sql(query, con=engine, coerce_float=False)
        print(Key_Products)

        yield Key_Products

