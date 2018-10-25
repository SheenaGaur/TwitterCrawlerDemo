# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["social_crawler_db"]

mycol = mydb["StreamRestMergedCollection"]

x = mycol.find_one()

print(x)