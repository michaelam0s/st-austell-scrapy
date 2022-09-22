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

class RobinsonsSites(scrapy.Spider):
    name = 'robinsons-sites-formatted'

    site_out = []

    def start_requests(self):
        yield Request(
            method='GET',
            url='https://api.robinsons.brew-systems.co.uk/api/v1/houses?fields=basic,urls,facilities&perPage=500',
            headers={
                'Connection': 'keep-alive',
                'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
                'Accept': 'application/json',
                'DNT': '1',
                'Authorization': 'Bearer 3T82rxpj%OWucQSX4iNT',
                'sec-ch-ua-mobile': '?0',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
                'Origin': 'https://www.robinsonsbrewery.com',
                'Sec-Fetch-Site': 'cross-site',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://www.robinsonsbrewery.com/',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8'
            },
            callback=self.parse_detail
        )

    def parse_detail(self, response):
        json_response = json.loads(response.body)
        site_list = json_response['data']

        if Testmode is True:
            site_list = site_list[:5]

        for sites in site_list:
            site = {}
            site['Address Line 1'] = sites['address1']
            site['Company'] = 'Robinsons Brewery'
            site['Country'] = 'UK'
            site['County'] = sites['area']
            site['Date'] = Date
            try:
                site['Format'] = sites['brand']['name']
            except:
                site['Format'] = 'Robinsons'
            site['Given ID'] = sites['id']
            site['Latitude'] = sites['latitude']
            site['Longitude'] = sites['longitude']
            site['Other Town'] = sites['town']
            site['Postcode'] = sites['postcode']
            site['Site Name'] = sites['name']
            site['Site Number'] = f'''RB-{sites['id']}'''
            site['Status'] = 'OPEN'
            site['Telephone Number'] = ''
            site['Town'] = sites['town']

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