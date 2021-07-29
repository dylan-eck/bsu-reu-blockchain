import networkx as nx
from networkx.algorithms import centrality

file_path = '../results/control/week_1/pf_graph.pickle'
with open(file_path, 'rb') as input_file:
    pf_graph = nx.read_gpickle(input_file)

centralities = nx.degree_centrality(pf_graph)

for item in centralities.values():
    print(f'{item:.2e}')
