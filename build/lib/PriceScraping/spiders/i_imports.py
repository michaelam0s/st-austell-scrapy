# -*- coding: utf-8 -*-
import re
import io
import json
import time

import scrapy
import random
import datetime

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request