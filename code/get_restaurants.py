from yelp_api import request
from pymongo import MongoClient
from pymongo import errors
import time
import json
import cPickle as pickle

'''
The purpose of this class is to get all of the restaurant businesses in San Francisco so that I can then use yelp_scraper to loop through and retrieve relevant users(reviewers) and reviews for each restaurant. Insert into MongoDB
'''

class get_restaurants(object):
    def __init__(self, collection, params):
        self.collection = collection
        self.params = params
        self.path = '/v2/search/'

    def make_request(self):
        return request(path=self.path, url_params=self.params)

    def insert_business(self, restaurant):
        # If the business is not already in our database, we need to store it
        if not self.collection.find_one({'id':restaurant['id']}):
            for field, val in restaurant.iteritems():
                if type(val) == str:
                    rest[field] = val.encode('utf-8')
            try:
                print "\n INSERTING RESTAURANT: " + restaurant['name']
                self.collection.insert(restaurant)
                self.collection.update({'id':restaurant['id']}, {'$set':{'type': 'business'}})
            except errors.DuplicateKeyError:
                print 'DUPLICATE'
        else:
            print "\n IN COLLECTIN ALREADY"

    def run(self):
        '''
        For loops through each restaurant category and inserts business information into MongoDB.
        '''
        try:
            response = self.make_request()
            total_num = response['total']
            print '\n TOTAL RESTAURANTS: %d \n' %total_num
            while self.params['offset'] < total_num:
                print 'OFFSET:', self.params['offset'], '\n'
                response = self.make_request()
                try:
                    for business in response['businesses']:
                        self.insert_business(business)
                except:
                    print 'EXCEPTION: TOO MANY RESTAURANTS IN CATEGORY: %s' %(self.params['category_filter'])
                self.params['offset'] += 20
                time.sleep(1.0)
        except Exception as e:
            print 'ERROR: %s for CATEGORY: %s ' %(e, self.params['category_filter'])


def main():
    ''' Commands to create MongoDB
    # client = MongoClient()
    # yelp_scrape = client['yelp']
    # yelp_scrape.create_collection('businesses')
    '''
    '''
    # Commands to create categories list from yelp categories json file
    with open('categories.json') as data_file:
        data = json.loads(data_file.read())
    categories = []
    for i in data:
        if i['parents'] == ['restaurants']:
            categories.append(i['alias'])
    with open('categories.pickle', 'wb') as handle:
        pickle.dump(categories, handle)
    '''

    DATABASE = 'yelp'
    COLLECTION = 'businesses'

    client = MongoClient()
    database = client[DATABASE]
    collection = database[COLLECTION]

    with open('../data/categories.pickle', 'rb') as handle:
        categories = pickle.load(handle)

    for category in categories:
        print category
        params = {'location': 'San+Francisco',
                  'term': 'restaurants',
                  'category_filter': category,
                  'limit': 20,
                  'offset': 0
                  }
        yelp = get_restaurants(collection, params)
        yelp.run()
        # break

if __name__ == '__main__':
    main()
