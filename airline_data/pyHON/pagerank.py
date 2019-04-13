# computes pagerank of network generated as HON
# Input: HON network file
# Output: PageRank for every node

import networkx as nx

def pagerank(network_filename, output_pagerank_filename):
    G = nx.DiGraph()

    with open(network_filename) as f:
        for line in f:
            fields = line.strip().split(',')
            eFrom = fields[0]
            eTo = fields[1]
            eWeight = float(fields[2])
            G.add_edge(eFrom, eTo, weight = eWeight)

    print('Computing PageRank...')
    pr = nx.pagerank(G, alpha=0.85, weight = 'weight', tol=1e-09, max_iter=1000)
    RealPR = {}

    print('Converting PageRank...')

    for node in pr:
        fields = node.split('|')
        FirstOrderNode = fields[0]
        if not FirstOrderNode in RealPR:
            RealPR[FirstOrderNode] = 0
        RealPR[FirstOrderNode] += pr[node]

    print('Writing PageRank...')

    nodes = sorted(RealPR.keys(), key=lambda x: RealPR[x], reverse=True)

    with open(output_pagerank_filename, 'w') as f:
        for node in nodes:
            f.write(node + ',' + str(RealPR[node]) + '\n')
