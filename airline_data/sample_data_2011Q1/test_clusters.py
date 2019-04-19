import pandas as pd
communities = pd.read_csv('hon_infomap.clu', sep=' ', skiprows=2, header=None, names=['nodeID', 'community', 'PageRank'])
communities.columns = ['nodeID', 'community', 'PageRank']
grouped = communities.groupby(['nodeID'])
node_cluster_lists = [list(communities.community[grouped.groups[j]]) for j in grouped.groups]
node_cluster_lists = dict(zip(communities.nodeID.unique(), node_cluster_lists))
print(node_cluster_lists)
