# # -*- coding: utf-8 -*-
# import re
# import io
# import json
# import time
# import numpy as np
# import scrapy
# import random
# import datetime
# from datetime import datetime
# import pandas as pd

# import PriceScraping.spiders.i_helpers as helpers
# import PriceScraping.spiders.i_variables as variables

# import urllib.parse

# from pprint import pprint
# from scrapy.http import Request



# engine = variables.engine

# query = ''' select * from "PROD_WAREHOUSE"."TEMP"."Competitor_Pricing_All_Products" where "Company" = 'JD Wetherspoon' order by random() limit 1 '''
# Key_Products = pd.read_sql(query, con=engine, coerce_float=False)
# print(Key_Products)

