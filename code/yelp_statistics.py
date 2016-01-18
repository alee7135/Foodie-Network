import numpy as np
import pandas as pd
import pickle
from pymongo import MongoClient
from collections import Counter, defaultdict
import time
from scipy.optimize import minimize
import multiprocessing
import threading

'''
Purpose of this module is to compute some descriptive statistics which will be plotted in the app
'''

client = MongoClient()
db = client['yelp']
coll = db['yelp_500_plus']

businesses = coll.distinct('business_id', {'type':'review'})
business_ratings = defaultdict()
business_votes = defaultdict()

for business in businesses:

    ## Compute the distribution of businesses
    temp = list(coll.find({'type':'review', 'business_id':business}, {'rating':1, 'votes':1}))
    ratings = Counter([i['rating'] for i in temp]).most_common()
    ratings.sort(key=lambda x: x[0], reverse=True)
    business_ratings[business] = [i[1] for i in ratings]

    ## Compute the distribution of votes
    votes = [i['votes'] for i in temp]
    funny = 0
    useful = 0
    cool = 0
    for i in votes:
        funny += i['funny']
        useful += i['useful']
        cool += i['cool']
    business_votes[business] = [useful, funny, cool]
    # print business_votes[business]

# Dump results so we can simply re-use them later in app.
pickle.dump(business_ratings, open( "../data/business_ratings.p", "wb"  ))
pickle.dump(business_votes, open( "../data/business_votes.p", "wb"  ))
