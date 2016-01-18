import pickle
from collections import Counter
import numpy as np
from igraph import *
# import graph_tool.all as gt
from collections import Counter, defaultdict
import time
from scipy.optimize import minimize
import multiprocessing
import threading

user_ = pickle.load(open("../data/user_edges.p", 'rb' ))
business_ = pickle.load(open("../data/business_edges.p", 'rb' ))
review_ = pickle.load(open("../data/review_edges.p", 'rb' ))
# cruis_ = pickle.load(open("../data/cruis_edges.p", 'rb' ))

dict_all_users_of_a_business = pickle.load(open("../data/dict_all_users_of_a_business.p", 'rb'  ))
dict_all_businesses_of_a_user = pickle.load(open("../data/dict_all_businesses_of_a_user.p", 'rb'  ))

user_edge_weight_ = pickle.load(open("../data/user_edge_weight.p", 'rb' ))
business_edge_weight_ = pickle.load(open("../data/business_edge_weight.p", 'rb' ))

class yelp_network(object):
    def __init__(self):
        self.g = Graph()
        # Create dictionaries to store the vertex node objects
        self.users_vertex = defaultdict()
        self.businesses_vertex = defaultdict()
        # Create dictionaries to store the edge node objects
        self.user_edges = defaultdict()
        self.business_edges = defaultdict()
        self.review_edges = defaultdict()
        # self.cruis_edges = defaultdict()
        self.users = dict_all_businesses_of_a_user.keys()
        self.num_users = len(dict_all_businesses_of_a_user.keys())
        self.businesses = dict_all_users_of_a_business.keys()
        self.num_businesses = len(dict_all_users_of_a_business.keys())
        self.base_mat = None
        self.users_mat_dic = defaultdict(int)
        self.businesses_mat_dic = defaultdict(int)
        self.avg_restaurant_rating = None
        self.mat_community = None
        self.user_community_vertices = None
        self.weights = None
        self.as_clust = None  # Contains the global network community object and modularity
        self.bus_community_user_vertices = defaultdict() # contains the user community of a single restaurant
        self.bus_community_bus_vertices = defaultdict()
        self.bus_community_membership = defaultdict()  # membership array for single business
        self.mat_community = np.zeros((self.num_users, self.num_businesses))
        self.business_modularity = defaultdict()
        self.user_business_modularity = defaultdict()
        self.mse1 = []


    def create_network(self):
        start = time.time()
        #pool = multiprocessing.Pool(16)
        review_vertex_counter = 0
        review_edge_counter = 0
        for user, business, rating, cruisine in review_:
        #     print user, business, rating, cruisine
            # user = user.encode("utf-8")
            # business = business.encode("utf-8")
            if user not in self.users_vertex:
                # vu = self.g.add_vertex() # vertex of user
                self.g.add_vertex(name='user')
                self.users_vertex[user] = review_vertex_counter
                # self.users_vertex[user] = self.g.vs.select(name=user)[0].index
                self.g.vs[self.users_vertex[user]]['user_id'] = user
                review_vertex_counter += 1
            if business not in self.businesses_vertex:
                # vb = self.g.add_vertex() # vertex of business
                self.g.add_vertex(name='business')
                self.businesses_vertex[business] = review_vertex_counter
                # self.businesses_vertex[business] = self.g.vs.select(name=business)[0].index
                # self.g.vs.select(name_eq=business)[0]['cruisine'] = cruisine
                self.g.vs[self.businesses_vertex[business]]['business_id'] = business
                self.g.vs[self.businesses_vertex[business]]['cruisine'] = cruisine
                review_vertex_counter += 1
            if (user, business) not in self.review_edges:
                # er = self.g.add_edge(vu,vb) # edge of review (USER,BUSINESS)

                self.g.add_edge(self.users_vertex[user], self.businesses_vertex[business], name='review')
                self.review_edges[(user, business)] = review_edge_counter
                # self.review_edges[(user, business)] = self.g.es.select(name='%s,%s' %(user,business))[0].index
                # self.g.es.select(name='%s,%s' %(user,business))[0]['rating'] = rating
                self.g.es[self.review_edges[(user, business)]]['rating'] = rating
                # self.g.es.select(name='%s,%s' %(user,business))[0]['review_id'] = (user, business)
                self.g.es[self.review_edges[(user, business)]]['review_id'] = (user, business)
                review_edge_counter += 1
                # print review_edge_counter
        for user1, user2 in user_:
            if (user1, user2) not in self.user_edges:
                # euu = self.g.add_edge(self.users_vertex[user1], self.users_vertex[user2])
                # self.g.edge_properties['user_user_id'] = self.euuprop
                # self.g.edge_properties['user_user_id'][euu] = (user1.encode("utf-8"), user2.encode("utf-8"))
                self.g.add_edge(self.users_vertex[user1],
                self.users_vertex[user2], name='user_user')
                self.user_edges[(user1, user2)] = review_edge_counter
                self.g.es[self.user_edges[(user1, user2)]]['user_user_id'] = (user1, user2)
                self.g.es[self.user_edges[(user1, user2)]]['weight'] = user_edge_weight_[(user1, user2)]
                review_edge_counter += 1
        for b1, b2 in business_:
            if (b1, b2) not in self.business_edges:
                # ec = g.add_edge(businesses_vertex[b1], businesses_vertex[b2])
                self.g.add_edge(self.businesses_vertex[b1], self.businesses_vertex[b2], name='bus_bus')
                self.business_edges[(b1, b2)] = review_edge_counter
                # g.edge_properties['cruisine_cat'] = ecprop
                # g.edge_properties['cruisine_cat'][ec] = (b1.encode("utf-8"), b2.encode("utf-8"))
                self.g.es[self.business_edges[(b1, b2)]]['bus_bus_id'] = (b1, b2)
                self.g.es[self.business_edges[(b1, b2)]]['weight'] = business_edge_weight_[(b1, b2)]
                review_edge_counter += 1

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


    # Create unique businesses and users so we can count how many rows and columns we needs
    def create_base_matrix(self):
        # Create user and business dictionaries so we can keep track of the indicies when filling in matrix

        for i,j in enumerate(self.users):
            self.users_mat_dic[j] = i
        for i,j in enumerate(self.businesses):
            self.businesses_mat_dic[j] = i

        # Create matrix of reviews, sparse matrix
        # Create empty matrix
        self.base_mat = np.zeros((self.num_users, self.num_businesses))
        # Iterate each user-business combo and insert the actual review value
        for user, bus, rating, cruisine in review_:
            self.base_mat[self.users_mat_dic[user], self.businesses_mat_dic[bus]] = rating

        # Compute the average restaurant review based on all users who reviewed each restaurant, which we use later to compute MSE
        self.avg_restaurant_rating = np.sum(self.base_mat, axis=0) / np.sum(self.base_mat!=0, axis=0)


    def compute_mse1(self):
        # Iterate each restaurant column to compute MSE but ONLY for reviews where there are values
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
            self.mse1.append(mse)
        self.mse1 = np.array(self.mse1)


    def validate_communities(self):
        ## Find the users
        fg = self.g.community_fastgreedy(weights='rating')  # need to add user-user weights
        self.as_clust = fg.as_clustering()
        print "Optimal GLOBAL Modularity ", self.as_clust.modularity
        print self.as_clust.summary()
        print "Communites Sizes: ", self.as_clust.sizes()
        self.g.vs['membership'] = self.as_clust.membership

        for user, bus, rating, cruisine in review_:
            ## We only need to repeat for each new business, each business under it will be the same
            self.bus_community_membership[bus] = self.g.vs['membership'][self.businesses_vertex[bus]]
            if bus not in self.bus_community_user_vertices:
                user_vertex_seq = self.g.vs.select(membership_eq=self.bus_community_membership[bus], name_eq='user') # all users in that business community
                self.bus_community_user_vertices[bus] = [i.index for i in user_vertex_seq]
                business_vertex_seq = self.g.vs.select(membership_eq=self.bus_community_membership[bus], name_eq='business') # all users in that business community
                self.bus_community_bus_vertices[bus] = [i.index for i in business_vertex_seq]

                delete_v = self.g.vs.select(lambda vertex: vertex.index not in self.bus_community_user_vertices[bus] and vertex.index != self.businesses_vertex[bus])
                g_copy = self.g.copy()
                g_copy.delete_vertices([i.index for i in delete_v])
                ratings = g_copy.es['rating']

            # if no community to base rating of off, we simply use the average
            if len(ratings) == 0:
                print 'Average Used'
                ratings = self.avg_restaurant_rating[self.businesses_mat_dic[bus]]
            print np.nanmean(ratings), len(ratings)
            self.mat_community[self.users_mat_dic[user],self.businesses_mat_dic[bus]] = np.nanmean(ratings)

        mse2 = []
        for i in xrange(self.num_businesses):
            mask = self.base_mat[:,i] != 0
            base_vector = self.base_mat[:,i][mask]
            mask = self.mat_community[:,i] != 0
            community_vector = self.mat_community[:,i][mask]

            mse = np.sum((base_vector - community_vector) ** 2)
            mse2.append(mse)
        mse2 = np.array(mse2)
        mse2 = mse2[mse2!=0]
        if len(mse2) > 0:
            cmse = np.nanmean(mse2[mse2!=0])
        else:
            cmse = 0
        print "Average MSE: ",  np.mean(self.mse1[self.mse1!=0])
        print "Community MSE: ", cmse
        # return np.nanmean(mse2[mse2!=0])

        return base_vector, community_vector, self.mat_community


    def gather_business_verticies_of_a_user(self, user):
        # Gather up all verticies related to this restaurant
        # The first vertex is the restaurant itself
    #     vertex_list = [businesses_vertex[business]]
        # Append all users of this business[business]
    #     users = dict_all_users_of_a_business[business]
        # Append all businesses of each user of this business
        vertex_list = [self.users_vertex[user]]
    #     for user in users:
    #     vertex_list.append(users[user])
        businesses = dict_all_businesses_of_a_user[user]
        for business in businesses:
            vertex_list.append(self.businesses_vertex[business])
        return vertex_list


    def compute_reweighted_user_bus_rating(self, user=u'/user_details?userid=mE-hdFF4RgCxspZU72GAsw', business=u'pho-huynh-hiep-2-kevins-noodle-house-san-francisco'):
        # Create filtered network including only the cluster which restaurant belongs and the restaurant's network
        # User provides a business and I take my master network and filter based on the community which the restaurant
        # belongs to AND on the user-business network.
        business_vertex_list = self.gather_business_verticies_of_a_user(user)

        u1 = self.g.vs.select(membership_ne=self.bus_community_membership[business])
        u2 = self.g.vs.select(lambda vertex: vertex.index not in business_vertex_list)
        u = list(set([i.index for i in u1] + [i.index for i in u2]))

        g_copy = self.g.copy()
        g_copy.delete_vertices(u)

        # gcc = g_copy.community_fastgreedy(weights='rating')
        # gcc = gcc.as_clustering()
        self.user_business_modularity[(user,business)] = np.mean(g_copy.closeness()) #g_copy.modularity(g_copy.vs['membership'], weights='rating')
        print "USER %s CLOSENESS: %f FOR BUSINESS %s VS. AVERAGE BUSINESS CLOSENESS: %f" %(user, self.user_business_modularity[(user,business)], business, self.business_modularity[business])



    def compute_reweighted_restaurant_rating(self, business=u'pho-huynh-hiep-2-kevins-noodle-house-san-francisco'):
        users = dict_all_users_of_a_business[business]
        # SAME as Previous, EXCEPT Need to compute modularity of component for business and ALL USERS who reviewed that restaurant and businesses they reviewed
        # BUSINESS COMPONENT (U/B) + BUSINESSES ALL USERS HAVE REVIEWED
        business_vertex_list = []
        for i in users:
            # Includes user in each business_vertex_list
            business_vertex_list.extend(self.gather_business_verticies_of_a_user(i))
        business_vertex_list = list(set(business_vertex_list))

        u1 = self.g.vs.select(membership_ne=self.bus_community_membership[business])
        u2 = self.g.vs.select(lambda vertex: vertex.index not in business_vertex_list)
        u = list(set([i.index for i in u1] + [i.index for i in u2]))

        g_copy = self.g.copy()
        g_copy.delete_vertices(u)

        self.business_modularity[business] = np.mean(g_copy.closeness())#g_copy.modularity(g_copy.vs['membership'])
        # assert(g_copy.vs['membership'])
        print "BUSINESS CLOSENESS: %f FOR %s VS. AVERAGE GLOBAL CLOSENESS: %f " %(self.business_modularity[business], business, np.mean(self.g.closeness()))

if __name__ == '__main__':
    start = time.time()
    print "\nLOADING PICKLES ..."
    G = yelp_network()
    print "\nLOADING YELP NETWORK ..."
    G.create_network()
    print "\nCREATING BASE MATRIX ..."
    G.create_base_matrix()
    print "\nCOMPUTING BASE MSE ..."
    G.compute_mse1()
    print "\nVALIDATING COMMUNITIES ..."
    a,b,c = G.validate_communities()
    print "\nCOMPUTING REWEIGHTED RESTAURANT OVERALL REVIEW ..."
    G.compute_reweighted_restaurant_rating() # This one must be computed first
    print "\nCOMPUTING REWEIGHTED REVIEW ..."
    G.compute_reweighted_user_bus_rating()

    myfile = open("../data/yelp_instance.p", "wb")
    pickle.dump(G, myfile, -1)
    myfile.close()
    end = time.time()
    print "Took %f seconds" %(end - start)
