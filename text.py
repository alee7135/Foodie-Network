import numpy as np
import pymongo
from datetime import datetime
import pandas as pd
import math
import json
import urllib
import urllib2
import oauth2
import os


class YelpDatabase(object):

    def __init__(self, database_name, cat_filt):
        """
        INPUT:
            database_name (str)
            cat_filt (str) - Yelp category filter, see:
             https://www.yelp.com/developers/documentation/v2/all_category_list
        OUTPUT:
            None.
        Initializes connection to MongoDB. Connects to data if the same search
        has been previously made.
        """
        self.database_name = database_name
        self.cat_filt = cat_filt
        self.coll = self._mongo_connect(dbn=self.database_name,
                                        coll_name=self.cat_filt)
        self.pull_count = 0
        self.full_grids = []

        # Order to sort API data. 2=highest rated according to Yelp.
        self.sort = 2

if __name__ == '__main__':
    test = YelpDatabase(datebase_name='yelp_test', cat_filt='restaurants')
    test.get_full_df()
