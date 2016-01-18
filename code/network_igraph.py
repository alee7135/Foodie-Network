import pickle
import numpy as np
from igraph import *
# import graph_tool.all as gt
from collections import Counter, defaultdict
import time
from scipy.optimize import minimize
import multiprocessing
import threading

'''
Import global variables for users and businesses
'''
user_ = pickle.load(open("../data/user_edges.p", 'rb' ))
business_ = pickle.load(open("../data/business_edges.p", 'rb' ))
review_ = pickle.load(open("../data/review_edges.p", 'rb' ))
# cruis_ = pickle.load(open("../data/cruis_edges.p", 'rb' ))

dict_all_users_of_a_business = pickle.load(open("../data/dict_all_users_of_a_business.p", 'rb'  ))
dict_all_businesses_of_a_user = pickle.load(open("../data/dict_all_businesses_of_a_user.p", 'rb'  ))



class yelp_network(object):
    '''
    Purpose of this yelp_network is to produce the graph network of users-businesses (reviews) as well as having methods for computing community for a single restaurant or single review.
    '''
    def __init__(self):
        self.g = Graph()
        # Create dictionaries to store the vertex node objects
        self.users_vertex = defaultdict()
        self.businesses_vertex = defaultdict()
        # Create dictionaries to store the edge node objects
        self.user_edges = defaultdict()
        self.business_edges = defaultdict()
        self.review_edges = defaultdict()
        self.cruis_edges = defaultdict()
        # Create other attributes which will need to be passed between methods
        self.users = dict_all_businesses_of_a_user.keys()
        self.num_users = len(dict_all_businesses_of_a_user.keys())
        self.businesses = dict_all_users_of_a_business.keys()
        self.num_businesses = len(dict_all_users_of_a_business.keys())
        self.base_mat = np.zeros((self.num_users, self.num_businesses))
        self.community_mat = np.zeros(self.num_businesses)
        self.users_mat_dic = defaultdict(int)
        self.businesses_mat_dic = defaultdict(int)
        self.avg_restaurant_rating = 0
        self.user_community_vertices = 0
        self.weights = 0
        self.as_clust = 0  # Contains the global network community object and modularity
        self.bus_community_user_vertices = defaultdict() # contains the user community of a single restaurant
        self.bus_community_bus_vertices = defaultdict()
        self.bus_community_membership = defaultdict()  # membership array for single business

        self.mse_base = []
        self.mse_community = []
        self.business_closeness = defaultdict() # closeness metric for each business (business community + all user's reviewed businesses)
        self.user_business_closeness = defaultdict() # closeness metric for a user-business (business community + single user's reviewed businesses)
        # Baseline to compare business_closeness metric/ user_business_closeness
        self.average_business_closeness = 0
        self.average_user_business_closeness = 0
        # Vertex list storer
        self.business_vertex_list_by_user = defaultdict()
        self.business_vertex_list_by_business = defaultdict(list)
        # most common cruisines by restaurant community
        self.most_common = defaultdict()
        self.least_common = defaultdict()
        self.restaurant_cruisine = defaultdict()


    def create_network(self):
        start = time.time()
        review_vertex_counter = 0
        review_edge_counter = 0
        for user, business, rating, cruisine in review_:
            if user not in self.users_vertex:
                self.g.add_vertex(name='user')
                self.users_vertex[user] = review_vertex_counter
                self.g.vs[self.users_vertex[user]]['user_id'] = user
                review_vertex_counter += 1
            if business not in self.businesses_vertex:
                self.g.add_vertex(name='business')
                self.businesses_vertex[business] = review_vertex_counter
                self.g.vs[self.businesses_vertex[business]]['business_id'] = business
                self.g.vs[self.businesses_vertex[business]]['cruisine'] = cruisine
                review_vertex_counter += 1
            if (user, business) not in self.review_edges:
                self.g.add_edge(self.users_vertex[user],self.businesses_vertex[business], name='review')
                self.review_edges[(user, business)] = review_edge_counter
                self.g.es[self.review_edges[(user, business)]]['rating'] = rating
                self.g.es[self.review_edges[(user, business)]]['review_id'] = (user, business)
                review_edge_counter += 1

        '''
        Below is additional edges for user-user, business-business, and cruisine-cruisine which could be used if I had enough time to compute the network (over 100 mil edges!)
        '''
        # for user1, user2 in user_:
        #     if (user1, user2) not in self.user_edges:
        #         # euu = self.g.add_edge(self.users_vertex[user1], self.users_vertex[user2])
        #         self.g.add_edge(self.users_vertex[user1], self.users_vertex[user2], name='user_user')
        #         self.user_edges[(user1, user2)] = review_edge_counter
        #         # self.g.edge_properties['user_user_id'] = self.euuprop
        #         # self.g.edge_properties['user_user_id'][euu] = (user1.encode("utf-8"), user2.encode("utf-8"))
        #         self.g.es[self.user_edges[(user1, user2)]]['user_user_id'] = (user1, user2)
        #
        #         review_edge_counter += 1
        # for b1, b2 in cruisine_:
        #     if (b1, b2) not in self.cruis_edges:
        #         # ec = g.add_edge(businesses_vertex[b1], businesses_vertex[b2])
        #         self.g.add_edge(self.businesses_vertex[b1], self.businesses_vertex[b2], name='bus_bus')
        #         self.cruis_edges[(b1, b2)] = review_edge_counter
        #         # g.edge_properties['cruisine_cat'] = ecprop
        #         # g.edge_properties['cruisine_cat'][ec] = (b1.encode("utf-8"), b2.encode("utf-8"))
        #         self.g.es[self.cruis_edges[(b1, b2)]['cruisine_cat'] = (b1, b2)
        #
        #         review_edge_counter += 1

        end = time.time()
        print "Took %f seconds" %(end - start)
        print 'Edges: %d' %self.g.ecount()
        print 'Vertices: %d' %self.g.vcount()
        print self.g.summary()


    def create_base_matrix(self):
        '''
        Create matrix of user-business ratings.
        '''
        # Create user and business dictionaries so we can keep track of the indicies when filling in matrix
        for i,j in enumerate(self.users):
            self.users_mat_dic[j] = i
        for i,j in enumerate(self.businesses):
            self.businesses_mat_dic[j] = i

        # Iterate each user-business combo and insert the actual review value
        for user, bus, rating, cruisine in review_:
            self.base_mat[self.users_mat_dic[user], self.businesses_mat_dic[bus]] = rating

        # Compute the average restaurant review based on all users who reviewed each restaurant, which we use later to compute MSE
        self.avg_restaurant_rating = np.sum(self.base_mat, axis=0) / np.sum(self.base_mat!=0, axis=0)

        # Feature Scaling
        temp = ((self.base_mat - self.avg_restaurant_rating) / len(self.avg_restaurant_rating)) + self.avg_restaurant_rating

        self.avg_restaurant_rating = np.sum(temp, axis=0) / np.sum(temp!=0, axis=0)

    def compute_mse_base(self):
        '''
        Iterate each restaurant column to compute MSE for the average rating for a business and the actual rating for each user
        Output: Updates mse_base - a list of mean-square values per business
        '''
        for i in xrange(self.num_businesses):
            # take only non-zero column values
            mask = self.base_mat[:,i] != 0
            base_vector = self.base_mat[:,i][mask]
            # compute the number of users who actually reviewed that restaurant
            col_len = sum(mask)
            # create a new vector for average restaurant ratings of length previously computed
            average_vector = np.zeros(col_len)
            # fill in this vector with constant value
            average_vector.fill(self.avg_restaurant_rating[i])
            # take different of vectors and square difference to get MSE

            mse = np.sum((base_vector - average_vector) ** 2)

            # Append this mse for this restaurant ONLY
            self.mse_base.append(mse)
        self.mse_base = np.array(self.mse_base)


    def validate_communities(self):
        '''
        In order to find value in the communities we compute later, we need to first validate and proof that our communities are actually better than the vanilla average. Here, we compute the user community average for each business and use that to estimate the rating each user would give.
        Output: Updates mse_community and compares to mse_base
        '''
        fg = self.g.community_fastgreedy(weights='rating')  # weights='rating' screws things up!
        self.as_clust = fg.as_clustering()
        print "Optimal GLOBAL Modularity ", self.as_clust.modularity
        print self.as_clust.summary()
        print "Communites Sizes: ", self.as_clust.sizes()
        # Very Important Step: Assign Membership!
        self.g.vs['membership'] = self.as_clust.membership

        for bus in self.businesses:
            # Compute the average of the community which each business belongs to
            self.bus_community_membership[bus] = self.g.vs['membership'][self.businesses_vertex[bus]]

            user_vertex_seq = self.g.vs.select(membership_eq=self.bus_community_membership[bus]) # only users in that business community
            self.bus_community_user_vertices[bus] = [i.index for i in user_vertex_seq]

            # business_vertex_seq = self.g.vs.select(membership_eq=self.bus_community_membership[bus], name_eq='business') # all businesses in that business community
            # self.bus_community_bus_vertices[bus] = [i.index for i in business_vertex_seq]

            # Delete all verticies that not the ones we want leaving a reduced network
            delete_v = self.g.vs.select(lambda vertex: vertex.index not in self.bus_community_user_vertices[bus])
            g_copy = self.g.copy()
            g_copy.delete_vertices([i.index for i in delete_v])
            assert(len(g_copy.vs)>0)
            assert(len(g_copy.es)>0)
            ratings = np.array(g_copy.es['rating'])
            # Feature Scaling
            temp = (ratings  - np.mean(ratings))
            ratings = (temp / len(temp)) + np.mean(ratings)

            # if no community to base rating of off, we simply use the average
            if len(ratings) == 0:
                print 'Average Used'
                ratings = self.avg_restaurant_rating[self.businesses_mat_dic[bus]]
            print np.nanmean(ratings), len(ratings)
            # Finally, insert a mean rating for each business based on community
            self.community_mat[self.businesses_mat_dic[bus]] = np.nanmean(ratings)

        # Compute the mse between our base matrix and our computed matrix
        for i in xrange(self.num_businesses):
            mask = self.base_mat[:,i] != 0
            base_vector = self.base_mat[:,i][mask]
            # mask = self.community_mat[i] != 0
            community_vector = self.community_mat[i]

            mse = np.sum((base_vector - community_vector) ** 2)
            self.mse_community.append(mse)
        self.mse_community = np.array(self.mse_community)
        self.mse_community = self.mse_community[self.mse_community!=0]
        if len(self.mse_community) > 0:
            cmse = np.nanmean(self.mse_community)
        else:
            cmse = 0
        print "Average MSE: ",  np.mean(self.mse_base[self.mse_base!=0])
        print "Community MSE: ", cmse


    def gather_business_verticies_of_a_user(self):
        '''
        Gather up all businesses verticies each user reviewed.
        '''
    #     vertex_list = [businesses_vertex[business]]
        # Append all users of this business[business]
    #     users = dict_all_users_of_a_business[business]
    #     for user in users:
    #     vertex_list.append(users[user])
        # Append all businesses of each user of this business
        for user in self.users:
            # all business ids of a user
            businesses = dict_all_businesses_of_a_user[user]
            vl = self.g.vs.select(name_eq='business', business_id_in=businesses)
            self.business_vertex_list_by_user[user] = [i.index for i in vl]
            # make sure we didnt make an error and that there is indeed data here
            assert(len(self.business_vertex_list_by_user[user]) > 0)


    def gather_business_verticies_of_a_business(self):
        '''
        Gather up all businesses verticies which all users of a restaurant reviewed. Basically doing what we did up there by restaurant.
        '''
        for business in self.businesses:
            users = dict_all_users_of_a_business[business]
            for user in users:
                # Includes user in each business_vertex_list
                self.business_vertex_list_by_business[business].extend(self.business_vertex_list_by_user[user])
            # make sure we didnt make an error and that there is indeed data here
            assert(len(self.business_vertex_list_by_business[business]) > 0)


    def compute_reweighted_user_bus_rating(self,user=u'/user_details?userid=mE-hdFF4RgCxspZU72GAsw',business=u'pho-huynh-hiep-2-kevins-noodle-house-san-francisco'):
        '''
        User provides a business and I take my master network and filter based on the community which the restaurant
        belongs to AND on all the businesses which this user reviewed
        '''
        # business_vertex_list = self.gather_business_verticies_of_a_user(user)
        # Modify the original business_vertex_list_by_user to include the user vertex because even though user reviewed the business, it doesn't guarentee that he/she is in same community as the business

        u1 = self.g.vs.select(membership_eq=self.bus_community_membership[business], user_id_eq=self.users_vertex[user])
        u2 = self.g.vs.select(user_id_eq=user)
        u3 = self.g.vs.select(lambda vertex: vertex.index in self.business_vertex_list_by_user[user])
        # This is all the verts we want
        temp = list(set([i.index for i in u1] + [i.index for i in u2]+ [i.index for i in u3]))
        # We want to select and delete everything that is not in this set
        u = self.g.vs.select(lambda vertex: vertex.index not in temp)
        g_copy = self.g.copy()
        g_copy.delete_vertices(u)

        assert(len(g_copy.vs)>0)
        assert(len(g_copy.es)>0)

        # gcc = g_copy.community_fastgreedy(weights='rating')
        # gcc = gcc.as_clustering()
        self.user_business_closeness[(user,business)] = np.mean(g_copy.closeness()) #g_copy.modularity(g_copy.vs['membership'], weights='rating')
        print "USER %s CLOSENESS: %f FOR BUSINESS %s VS. AVERAGE BUSINESS CLOSENESS: %f" %(user, self.user_business_closeness[(user,business)], business, self.business_closeness[business])


    def compute_reweighted_restaurant_rating(self, business=u'pho-huynh-hiep-2-kevins-noodle-house-san-francisco'):
        '''
        Take user input for restaurant and compute community for the restaurant. It is the exact same function as previous except we don't iterate every user.
        '''

        # SAME as Previous, EXCEPT Need to compute modularity of component for business and ALL USERS who reviewed that restaurant and businesses they reviewed
        # BUSINESS COMPONENT (U/B) + BUSINESSES ALL USERS HAVE REVIEWED
        # business_vertex_list = []


        # Similarily to above, we need to add all the users verticies to the business vertex list
        users = dict_all_users_of_a_business[business]
        business_vertex_list = self.business_vertex_list_by_business[business]

        u1 = self.g.vs.select(membership_eq=self.bus_community_membership[business])
        u2 = self.g.vs.select(user_id_in=users)
        u3 = self.g.vs.select(lambda vertex: vertex.index in business_vertex_list)
        temp = list(set([i.index for i in u1] + [i.index for i in u2]+ [i.index for i in u3]))
        u = self.g.vs.select(lambda vertex: vertex.index not in temp)
        g_copy = self.g.copy()
        g_copy.delete_vertices(u)

        assert(len(g_copy.vs)>0)
        assert(len(g_copy.es)>0)

        self.business_closeness[business] = np.mean(g_copy.closeness()) #g_copy.modularity(g_copy.vs['membership'])
        # assert(g_copy.vs['membership'])
        print "BUSINESS CLOSENESS: %f FOR %s VS. AVERAGE GLOBAL CLOSENESS: %f " %(self.business_closeness[business], business, np.mean(self.g.closeness()))


    def find_most_least_common_cruisines_of_business_community(self, nofind=5):
        '''
        One we know how similar a particular business's community is, we can make sense of the say top 5 or 10 most common cruisines or least common.
        '''
        # Create a dictionary for each restaurant and its cruisine types
        for i in self.g.vs.select(name_eq='business'): #352 unique cr
            self.restaurant_cruisine[self.g.vs[i.index]['business_id']]= self.g.vs[i.index]['cruisine']
        # For each business, we want to find the top N cruisines by restaurant
        for business in self.businesses:
            # Get all cruisines of businesess within a business community
            cruisines = self.g.vs[self.business_vertex_list_by_business[business]]['cruisine']
            # Now find the nofind most common
            self.most_common[business] = Counter(cruisines).most_common()[0:nofind]
            # Now find the nofind least common
            self.least_common[business] = list(reversed(Counter(cruisines).most_common()))[0:nofind]


    def find_most_least_common_cruisines_of_user(nofind=None):
        '''
        One we know how similar a particular business's community is, we can make sense of the say top 5 or 10 most common cruisines or least common.
        '''
        # Create a dictionary for each restaurant and its cruisine types
        for i in g.g.vs.select(name_eq='business'): #352 unique cr
            g.restaurant_cruisine[g.g.vs[i.index]['business_id']]= g.g.vs[i.index]['cruisine']
        # For each business, we want to find the top N cruisines by restaurant
        for user in g.users:
            # Get all cruisines of businesess within a business community
            cruisines = g.g.vs[g.business_vertex_list_by_user[user]]['cruisine']
            # Now find the nofind most common
            if nofind != None:
                g.most_common[user] = Counter(cruisines).most_common()[0:nofind]
                # Now find the nofind least common
                g.least_common[user] = list(reversed(Counter(cruisines).most_common()))[0:nofind]
            else:
                g.most_common[user] = Counter(cruisines).most_common()
                # Now find the nofind least common
                g.least_common[user] = list(reversed(Counter(cruisines).most_common()))
                print 'hello'
        return g.most_common, g.least_common


    def compute_all_business_closeness(self):
        '''
        Compute the average density or closeness of connections for each business separately. This is the baseline average which we can compare our users similarity metric for a given business. If for example, a given restaurant has higher closeness than the average, than the community of reviewers are more similar because they reviewed similar restaurants. Conversely, the community of reviewers were more diverse. Then we can compare with the most common cruisines to try to answer our original question.
        '''
        for business in self.businesses:
            # update the self.business_closeness dictionary for each business
            self.compute_reweighted_restaurant_rating(business)

        self.average_business_closeness = np.nanmean(self.business_closeness.values())

        print "AVERAGE BUSINESS CLOSENESS %f" %self.average_business_closeness


    def compute_all_user_business_closeness(self):
        '''
        Similar to above, compute the baseline closeness for all user-business relationships. For example, if we examine a single user-business connection and their closeness was higher than the average among all user-business connections, then this user is more legit than the average. He/she is more in sync with the restaurant's community than average.
        '''
        for user, business, rating, cruisine  in review_:
            # update the self.user_business_closeness dictionary for each business
            self.compute_reweighted_user_bus_rating(user, business)

        self.average_user_business_closeness = np.nanmean(self.user_business_closeness.values())

        print "AVERAGE USER-BUSINESS CLOSENESS %f" %self.average_user_business_closeness



if __name__ == '__main__':
    myfile = open( "../data/yelp_instance.p", "wb"  )

    start = time.time()
    print "\nLOADING PICKLES ..."
    G = yelp_network()
    print "\nLOADING YELP NETWORK ..."
    G.create_network()
    G.g.write_pickle("../data/graph.p")
    print "\nCREATING BASE MATRIX ..."
    G.create_base_matrix()
    print "\nCOMPUTING BASE MSE ..."
    G.compute_mse_base()
    print "\nVALIDATING COMMUNITIES ..."
    G.validate_communities()
    print "\nGATHERING VERTICES ..."
    G.gather_business_verticies_of_a_user()
    G.gather_business_verticies_of_a_business()
    pickle.dump(G, myfile, -1)
    print "\nCOMPUTING REWEIGHTED BUSINESS CLOSENESS ..."
    G.compute_all_business_closeness() # compute closeness per business
    pickle.dump(G, myfile, -1)
    print "\nCOMPUTING MOST COMMON/ LEAST COMMON CRUISINES PER BUSINESS ..."
    G.find_most_least_common_cruisines_of_business_community(nofind=100)
    pickle.dump(G, myfile, -1)
    # Compute this very last because it may not finish
    print "\nCOMPUTING REWEIGHTED USER-BUSINESS CLOSENESS..."
    G.compute_all_user_business_closeness() # compute closeness per review

    pickle.dump(G, myfile, -1)
    myfile.close()

    end = time.time()
    print "Took %f seconds" %(end - start)
