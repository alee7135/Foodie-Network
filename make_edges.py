from pymongo import MongoClient
from collections import defaultdict
from itertools import combinations

def load_data():
    '''
    filename: name of imdb edge data file
    Read in the data and create two dictionaries of adjacency lists, one for
    the actors and one for the movies.
    '''
    client = MongoClient()
    db = client['yelp']
    coll = db['businesses']

    edges = coll.find({'type':'review'}, {'user_id':1, 'business_id':1})

    users = defaultdict(set)
    businesses = defaultdict(set)
    for dic in edges:
        user = dic['user_id']
        business = dic['business_id']
        users[user].add(business)
        businesses[business].add(user)
    return users, businesses


def make_edge_file(filename, d):
    '''
    filename: name of file to write to
    d: dictionary of edge data
    Write edge list to the file.
    '''
    f = open(filename, 'w')
    edges = set()
    for key, values in d.iteritems():
        for edge in combinations(values, 2):
            edges.add(tuple(sorted(edge)))

    for one, two in edges:
        f.write("%s\t%s\n" % (one, two))
    f.close()


if __name__ == '__main__':
    users, businesses = load_data()
    make_edge_file('data/business_edges.tsv', users)
    make_edge_file('data/user_edges.tsv', businesses)

# class network(object):
