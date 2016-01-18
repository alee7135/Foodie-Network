import pickle
from collections import Counter
import numpy as np
import graph_tool.all as gt
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

# def get_num_components(G):
#     comp, hist = gt.label_components(G)
#     num_comp = len(np.array(comp.a))
#     return num_comp
# def girvan_newman_step(G):
#     '''
#     INPUT: Graph G
#     OUTPUT: None
#     Run one step of the Girvan-Newman community detection algorithm.
#     Afterwards, the graph will have one more connected component.
#     '''
#     # init_ncomp = nx.number_connected_components(G)
#     # init_ncomp = get_num_components(G)
#     init_ncomp = get_num_components(G)
#     ncomp = init_ncomp
#     # while the numb of components has not changed we keep removing edges and we end up with an updated G graph
#     while ncomp == init_ncomp:
#         vertex_betweenness, edge_betweenness = gt.betweenness(G)
#         # bw = Counter(nx.edge_betweenness_centrality(G))
#         # a, b = bw.most_common(1)[0][0]
#         # G.remove_edge(a, b)
#
#         init_list = list(G.edges())
#         max_edge_list = np.argsort(edge_betweenness.a)[:-1][0:1000]
#         for i in max_edge_list:
#             G.remove_edge(init_list[i])
#         # ncomp = nx.number_connected_components(G)
#         print G.num_edges()
#         ncomp = get_num_components(G)
#         print ncomp
# def find_communities_n(G, n):
#     '''
#     INPUT: Graph G, int n
#     OUTPUT: list of lists
#     Run the Girvan-Newman algorithm on G for n steps. Return the resulting
#     communities.
#     '''
#     G1 = G.copy()
#     for i in xrange(n):
#         girvan_newman_step(G1)
#     return list(nx.connected_components(G1))
# def find_communities_modularity(G, max_iter=None):
#     '''
#     INPUT:
#         G: networkx Graph
#         max_iter: (optional) if given, maximum number of iterations
#     OUTPUT: list of lists of strings (node names)
#     Run the Girvan-Newman algorithm on G and find the communities with the
#     maximum modularity.
#     '''
#     # degrees = G.degree()
#     # num_edges = G.number_of_edges()
#     # num_edges = G.num_edges()
#     G1 = G.copy()
#     best_modularity = -1.0
#     # best_comps = nx.connected_components(G1)
#
#     i = 0
#     initial_components = gt.label_components(G1)
#     while G1.num_edges() > 0:
#         # subgraphs = nx.connected_component_subgraphs(G1)
#         # modularity = get_modularity(subgraphs, degrees, num_edges)
#         # print initial_components
#         # b = gt.community_structure(G1, 10000, 10)
#         components = gt.label_components(G1)[0]
#         modularity = gt.modularity(G1, components)
#         print modularity
#         if modularity > best_modularity:
#             best_modularity = modularity
#             best_comps, hist = gt.label_components(G1)
#         girvan_newman_step(G1)
#         i += 1
#         if max_iter and i >= max_iter:
#             break
#     return best_comps, best_modularity
# def get_modularity(subgraphs, degrees, num_edges):
#     '''
#     INPUT:
#         subgraphs: graph broken in subgraphs
#         degrees: dictionary of degree values of original graph
#         num_edges: float, number of edges in original graph
#     OUTPUT: Float (modularity value, between -0.5 and 1)
#     Return the value of the modularity for the graph G.
#     '''
#     mod = 0
#     for g in subgraphs:
#         for node1 in g:
#             for node2 in g:
#                 mod += int(g.has_edge(node1, node2))
#                 mod -= degrees[node1] * degrees[node2] / (2. * num_edges)
#     return mod / (2. * num_edges)

class yelp_network(object):
    def __init__(self):
        self.g = gt.Graph(directed=False)
        self.vuprop =  self.g.new_vertex_property("string") # prop for user
        self.vbprop =  self.g.new_vertex_property("string") # prop for business
        self.vcprop = self.g.new_vertex_property("vector<string>") # prop for cruisine
        self.erprop = self.g.new_edge_property('int16_t') # prop for rating
        self.euuprop = self.g.new_edge_property("vector<string>") # prop for user-user
        self.ebbprop = self.g.new_edge_property("vector<string>") # prop for business-business
        self.eubprop = self.g.new_edge_property("vector<string>") # prop for user-business
        #self.ecprop = g.new_edge_property("vector<string>") # prop for business-business by same cruisine
        # Create dictionaries to store the vertex node objects
        self.users_vertex = defaultdict()
        self.businesses_vertex = defaultdict()
        # Create dictionaries to store the edge node objects
        self.user_edges = defaultdict()
        self.business_edges = defaultdict()
        self.review_edges = defaultdict()
        self.cruis_edges = defaultdict()
        self.b = None
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

    def create_network(self):
        start = time.time()
        #pool = multiprocessing.Pool(16)

        for user, business, rating, cruisine in review_:
        #     print user, business, rating, cruisine
            if user not in self.users_vertex:
                vu = self.g.add_vertex() # vertex of user
                self.users_vertex[user] = vu
                self.g.vertex_properties['user_id'] = self.vuprop
                self.g.vertex_properties['user_id'][vu] = user.encode("utf-8")
            if business not in self.businesses_vertex:
                vb = self.g.add_vertex() # vertex of business
                self.businesses_vertex[business] = vb
                self.g.vertex_properties['business_id'] = self.vbprop
                self.g.vertex_properties['business_id'][vb] = business.encode("utf-8")
                self.g.vertex_properties['cruisine'] = self.vcprop
                self.g.vertex_properties['cruisine'][vb] = cruisine
            if (user, business) not in self.review_edges:
                er = self.g.add_edge(vu,vb) # edge of review (USER,BUSINESS)
                self.review_edges[(user, business)] = er
                self.g.edge_properties['rating'] = self.erprop
                self.g.edge_properties['rating'][er] = rating
                self.g.edge_properties['review_id'] = self.eubprop
                self.g.edge_properties['review_id'][er] = (user.encode("utf-8"), business.encode("utf-8"))

        # for user1, user2 in user_:
        #     if (user1, user2) not in self.user_edges:
        #         euu = self.g.add_edge(self.users_vertex[user1], self.users_vertex[user2])
        #         self.user_edges[(user1, user2)] = euu
        #         self.g.edge_properties['user_user_id'] = self.euuprop
        #         self.g.edge_properties['user_user_id'][euu] = (user1.encode("utf-8"), user2.encode("utf-8"))

        # for b1, b2 in cruisine_:
        #     if (b1, b2) not in cruis_edges:
        #         ec = g.add_edge(businesses_vertex[b1], businesses_vertex[b2])
        #         cruis_edges[(b1, b2)] = ec
        #         g.edge_properties['cruisine_cat'] = ecprop
        #         g.edge_properties['cruisine_cat'][ec] = (b1.encode("utf-8"), b2.encode("utf-8"))

        end = time.time()
        print "Took %f seconds" %(end - start)
        print 'Edges: %d' %self.g.num_edges()
        print 'Vertices: %d' %self.g.num_vertices()
        print self.g.list_properties()

    def optimize_global_modularity(self, x):
        b = gt.community_structure(self.g, 100, x)
        return -1*(gt.modularity(self.g, b))

    def compute_global_community(self):
        r = []
        for i in xrange(3):
            r.append(minimize(self.optimize_global_modularity, 0, method='COBYLA', options={'disp': False}).x)
        print "Optimal global group number: ", np.mean(r)
        b = gt.community_structure(self.g, 100, np.mean(r))
        return b

    def compute_local_community(self, g):
        r = []
        for i in xrange(3):
            r.append(minimize(lambda x: -1*(gt.modularity(g, gt.community_structure(g, 100, x)))
            , 0, method='COBYLA', options={'disp': True}).x)
        print "Optimal global Modularity: ", np.mean(r)
        b = gt.community_structure(g, 100, np.mean(r))
        return b

    def compute_global_blocks(self):
        state = gt.minimize_blockmodel_dl(self.g, verbose=True)
        b = state.b
        mod = gt.modularity(self.g, b)
        print mod
        return b

    def compute_local_blocks(self):
        state = gt.minimize_blockmodel_dl(self.g, verbose=True)
        b = state.b
        mod = gt.modularity(self.g, b)
        print mod
        return b

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
        for user, bus, rating, cruisine in review_[0:10]:
            self.base_mat[self.users_mat_dic[user],                    self.businesses_mat_dic[bus]] = rating

        # Compute the average restaurant review based on all users who reviewed each restaurant, which we use later to compute MSE
        self.avg_restaurant_rating = np.sum(self.base_mat, axis=0) / np.sum(self.base_mat!=0, axis=0)


    def compute_mse1(self):
        mse1 = []
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
            mse1.append(mse)
        mse1 = np.array(mse1)
        print "Average MSE: ",  np.mean(mse1[mse1!=0])
        # return np.mean(mse1[mse1!=0])


    def validate_communities(self):
        # Use optimal communities to validate new ratings
        self.b = self.compute_global_community()
        globmod = gt.modularity(self.g, self.b)
        print "Optimal GLOBAL Modularity: ", globmod
        self.g.vertex_properties['components'] = self.b

        self.user_community_vertices = defaultdict()
        self.mat_community = np.zeros((self.num_users, self.num_businesses))

        for user, bus, rating, cruisine in review_[0:10]:
            comm = self.g.vertex_properties['components'][self.users_vertex[user]]
            if user not in self.user_community_vertices:
                u = gt.GraphView(self.g, vfilt=lambda x: self.g.vertex_properties['components'][x] == comm and
                                                len(self.g.vertex_properties['user_id'][x]) > 0)
                self.user_community_vertices[user] = u.vertices()
            # need users vertex only which are in this community
            # then find the respective user-bus for each eacher within the community
            bus_vertex = self.businesses_vertex[bus]
            rate = []
            for i in self.user_community_vertices[user]:
                try:
        #             print i, bus_vertex
                    rating = self.g.edge_properties['rating'][self.g.edge(i, bus_vertex)]
                    rate.append(rating)
                except:
                    continue
            # if no community to base rating of off, we simply use the average
            if len(rate) == 0:
                print 'Average Used'
                rate = self.avg_restaurant_rating[self.businesses_mat_dic[bus]]
            self.mat_community[self.users_mat_dic[user],                  self.businesses_mat_dic[bus]] = np.nanmean(rate)

        mse2 = []
        for i in xrange(self.num_businesses):
            mask = self.base_mat[:,i] != 0
            base_vector = self.base_mat[:,i][mask]
            mask = self.mat_community[:,i] != 0
            community_vector = self.mat_community[:,i][mask]

            mse = np.sum((base_vector - community_vector) ** 2)
            mse2.append(mse)
        mse2 = np.array(mse2)
        print "Community MSE: ", np.nanmean(mse2[mse2!=0])
        # return np.nanmean(mse2[mse2!=0])


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


    def compute_modularity(self, user, bus):
        # Create filtered network including only the cluster which restaurant belongs and the restaurant's network
        # User provides a business and I take my master network and filter based on the community which the restaurant
        # belongs to AND on the user-business network.
        group = self.g.vertex_properties['components'][self.businesses_vertex[bus]]
        business_vertex_list = self.gather_business_verticies_of_a_user(user)
        u = gt.GraphView(self.g, vfilt=lambda x: self.g.vertex_properties['components'][x] == group or x in business_vertex_list)
        # state = gt.minimize_blockmodel_dl(u, eweight= u.edge_properties['rating'])
        # b = state.b
        b = self.compute_local_community(u)
        modularity = gt.modularity(u, b)
        print "LOCAL Modularity: ", modularity
        return modularity


    def compute_reweighted_rating(self, business=u'so-san-francisco-4'):
        '''
        Take user input for restaurant and compute the
        '''
        users = dict_all_users_of_a_business[business]
        self.weights = np.zeros(self.num_users)

        print "Iterating through %d USERS to REWEIGHT %s" %(len(users), business)
        for i in users:
            weight = self.rescale_value(self.compute_modularity(i, business))
            self.weights[self.users_mat_dic[i]] = weight

        col = self.businesses_mat_dic[business]
        # mask = self.base_mat[:,col] != 0
        base_vector = self.base_mat[:,col]#[mask]
        # mask = self.weights != 0
        weights_vector = self.weights#[mask]
        total_weight = np.sum(weights_vector)

        temp = (base_vector * weights_vector) / total_weight
        if np.any(temp):
            reweighted_vector = self.rescale_column(temp)
        else:
            print "Not enough reviews to reweight restaurant %s" %business
        print np.mean(base_vector), np.mean(reweighted_vector)
        return reweighted_vector

    #
    # def compute_mse2(self):
    #     # multipy each weight by the user rating
    #     mat_weights = np.zeros((self.num_users, self.num_businesses))
    #     # Create user-business weights ONLY where we have reviews
    #     count = 0
    #     for user, bus, rating, cruisine in review_[0:10]:
    #         count +=1
    #         # Compute the weight for every review
    #         weight = self.rescale_value(self.compute_modularity(user, bus))
    #         mat_weights[self.users_mat_dic[user], self.businesses_mat_dic[bus]] = weight
    #         print count, weight
    #
    #     #  Can move this up to mse1 function to compute together!
    #     mse2 = []
    #     for i in xrange(self.num_businesses):
    #         mask = self.base_mat[:,i] != 0
    #         base_vector = self.base_mat[:,i][mask]
    #         mask = mat_weights[:,i] != 0
    #         weights_vector = mat_weights[:,i][mask]
    #         total_weight = np.sum(weights_vector)
    #         # multiply the original matrix by our weights matrix and divide by the sum of weights per restaurant
    #         temp = (base_vector * weights_vector) / total_weight
    #         #     temp = (mat * mat_weights) / mat_weights.sum(axis=0)
    #         # Rescale the matrix rating to 1-5 after re-weighting
    #     #     mat_scaled = np.apply_along_axis(rescale, 0, temp)
    #         if np.any(temp):
    #             model_vector = self.rescale_column(temp)
    #         else:
    #             model_vector = temp
    #         mse = np.sum((base_vector - model_vector) ** 2)
    #         mse2.append(mse)
    #
    #
    #     # Take squared difference of original matrix and my scaled matrix, then average by restaurant
    #     # temp = np.mean((mat - mat_scaled) **2, axis=0)
    #     # Finally, take the average mean square error across all restaurants
    #     # ms2 = np.mean(temp)
    #     return np.nanmean(mse2)
    #
    #     # How do the 2 MSE compare? Have we improved recommended rating of a restaurant of any new reviewer?
    #     # Compare models: the better model will have a large difference between the 2 MSEs with my MSE being lower since the
    #     # standard MSE will never change.
    #     # Then we could predict reviews for a newcomer to a particular restaurant based on their relationship to communities.

    def rescale_column(self, column):
        OldMax = np.max(column)
        OldMin = np.min(column)
        NewMax = 5.0
        NewMin = 1.0
        OldRange = (OldMax - OldMin)
        NewRange = (NewMax - NewMin)
        NewValues = map(lambda x: (((x - OldMin) * NewRange) / OldRange) + NewMin, column)
        return NewValues

    def rescale_value(self, value):
        OldMax = 1.0
        OldMin = -1.0
        NewMax = 1.0
        NewMin = 0.0
        OldRange = (OldMax - OldMin)
        NewRange = (NewMax - NewMin)
        NewValue = (((value - OldMin) * NewRange) / OldRange) + NewMin
        return NewValue


if __name__ == '__main__':
    print "\nLOADING PICKLES ..."
    G = yelp_network()
    print "\nLOADING YELP NETWORK ..."
    G.create_network()
    print "\nCREATING BASE MATRIX ..."
    G.create_base_matrix()
    print "\nCOMPUTING BASE MSE ..."
    G.compute_mse1()
    print "\nVALIDATING COMMUNITIES ..."
    G.validate_communities()
    print "\nCOMPUTING REWEIGHTED REVIEW ..."
    G.compute_reweighted_rating()
