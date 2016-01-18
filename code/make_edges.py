from pymongo import MongoClient
from collections import defaultdict
from itertools import combinations, chain
import pickle
import numpy as np

client = MongoClient()
db = client['yelp']
coll = db['yelp_500_plus']


def create_edges():
    '''
    filename: name of imdb edge data file
    Read in the data and create two dictionaries of adjacency lists, one for
    the actors and one for the movies.
    '''

    #### Very important to NOTE that our collection only contains restaurants >= 500 reviews so some users will have definietly reviewed more restaurants but I only show that restaurants which have >= 500 reviews
    mongo_docs = coll.find({'type':'review'})
    documents = list(mongo_docs)
    users = defaultdict(set)
    businesses = defaultdict(set)
    cruisines = defaultdict(set)
    cruisine_business = defaultdict()
    reviews = []


    for doc in documents:
        user = doc['user_id']
        business = doc['business_id']
        print user, business, '\n'
        if business not in businesses:
            b = coll.find_one({'type':'business', 'id':business})
            cruisine = tuple(chain(*(b['categories'])))
            cruisines[cruisine].add(business)
            cruisine_business[business] = cruisine

        users[user].add(business)
        # print "USERS: ", users
        businesses[business].add(user)
        # print "BUSINESSES: ", businesses

        rating = doc['rating']
        # b = coll.find_one({'type':'business', 'id':business})
        # cruisine = list(chain(*(b['categories'])))
        cruisine = cruisine_business[business]
        reviews.append((user, business, rating, cruisine))
        # break
    return users, businesses
    # Create business-business edges based on same user
    make_edge_file('business_edges', users)
    # Create user-user edges based on same business
    make_edge_file('user_edges', businesses)
    # Create business-business edges based on same cruisine
    make_edge_file("cruis_edges", cruisines)
    # Create user-business (reviews) edges
    pickle.dump(reviews, open( "../data/review_edges.p", "wb"  ))
    # Pickle these dictionaries for later usage
    dict_all_users_of_a_business = pickle.dump(businesses, open("../data/dict_all_users_of_a_business.p", "wb"))
    dict_all_businesses_of_a_user = pickle.dump(users, open("../data/dict_all_businesses_of_a_user.p", "wb"))


    # User-User edge weights dict for
    user_edge_weight = defaultdict(int)
    # print len(list(combinations(users.keys(), 2)))
    # for each combination of users, how many restaurants did they review in common
    for c in combinations(users.keys(), 2):
        user_edge_weight[tuple(sorted(c))] = len(users[c[0]].intersection(users[c[1]])) + 1

    business_edge_weight = defaultdict(int)
    # Business-Business edge weights. Ex: mcdonalds and burger king were reviewed by 10 of the same people so there Business-Business connection should be upweighted by 10
    # print len(list(combinations(businesses.keys(), 2)))
    for c in combinations(businesses.keys(),2):
        business_edge_weight[tuple(sorted(c))] = len(businesses[c[0]].intersection(businesses[c[1]])) + 1


    pickle.dump(user_edge_weight, open( "../data/user_edge_weight.p", "wb"  ))
    pickle.dump(business_edge_weight, open( "../data/business_edge_weight.p", "wb"  ))

# def create_category_edges():
#     '''
#     filename: name of imdb edge data file
#     Read in the data and create two dictionaries of adjacency lists, one for
#     the actors and one for the movies.
#     '''
#
#     # user_docs = coll.find({'type':'review'}, {'user_id':1, 'business_id':1})
#     user_docs = coll.find({'type':'review'})
#     docs = list(user_docs)
#
#     master_lst = []
#     cruisines = defaultdict(set)
#     business_dic = set()
#
#     for dic in docs:
#         business = dic['business_id']
#         if business not in business_dic:
#             business_dic.add(business)
#             b = coll.find_one({'type':'business', 'id':business})
#             cruisine = tuple(chain(*(b['categories'])))
#             master_lst.append((business, cruisine))
#             cruisines[cruisine].add(business)
#
#     make_edge_file("cruisines.p", cruisines)

# def create_single_network(business, filename):
#     '''
#     filename: name of imdb edge data file
#     Read in the data and create two dictionaries of adjacency lists, one for
#     the actors and one for the movies.
#     '''
#
#     user_docs = coll.find({'type':'review', 'business_id':business})
#     reviews = []
#
#     for dic in user_docs:
#         user = dic['user_id']
#         business_docs = coll.find({'type':'review', 'user_id':user})
#         for rev in business_docs:
#             business = rev['business_id']
#             reviews.append((user, business))


def make_edge_file(filename, d):
    '''
    filename: name of file to write to
    d: dictionary of edge data
    Write edge list to the file.
    '''
    edges = set()  # However 2 users or businesses could theoretically be connected multiple times
    for key, values in d.iteritems():
        for edge in combinations(values, 2):
            edges.add(tuple(sorted(edge)))

    pickle.dump(edges, open("../data/%s.p" %filename, "wb"))

if __name__ == '__main__':
    users, businesses = create_edges()

    pickle.dump(users, open("../data/extra/users.p", 'wb'))
    pickle.dump(businesses, open("../data/extra/businesses.p", 'wb'))
    print '-------------------------------------------------------\n'
    review_edges = pickle.load(open("../data/review_edges.p", 'rb'  ))
    user_edges = pickle.load(open("../data/user_edges.p", 'rb'  ))
    user_edges_weights = pickle.load(open("../data/user_edge_weight.p", 'rb' ))
    business_edges = pickle.load(open("../data/business_edges.p", 'rb'  ))
    business_edges_weights = pickle.load(open("../data/business_edge_weight.p", 'rb'  ))
    cruis_edges = pickle.load(open("../data/cruis_edges.p", 'rb'  ))
    dict_all_users_of_a_business = pickle.load(open("../data/dict_all_users_of_a_business.p", 'rb'  ))
    dict_all_businesses_of_a_user = pickle.load(open("../data/dict_all_businesses_of_a_user.p", 'rb'  ))

    users = pickle.load(open("../data/extra/users.p", 'rb'  ))
    businesses = pickle.load(open("../data/extra/businesses.p", 'rb'  ))
