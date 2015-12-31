import networkx as nx

G_user = nx.read_edgelist('data/user_edges.tsv', delimiter='\t')
G_business = nx.read_edgelist('data/business_edges.txt', delimiter='\t')
