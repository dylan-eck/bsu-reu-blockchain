import networkx as nx
from networkx.classes.function import all_neighbors

# load graph
address_graph = nx.read_gpickle('../data_out/address_graph.pickle')

mtm_addrs = []
non_mtm_addrs = []
selected_addresses = []

for node in address_graph.nodes:
    if address_graph.nodes[node]['in_mtm']:
        mtm_addrs.append(node)
    
    else:
        non_mtm_addrs.append(node)

for address in mtm_addrs:
    if len(address_graph.adj[address]) > 0:
        for neighbor in address_graph.adj[address]: 
            if not address_graph.nodes[neighbor]['in_mtm']:
                selected_addresses.append(neighbor)

print(f'      mtm addreses: {len(mtm_addrs):,}')
print(f' non mtm addresses: {len(non_mtm_addrs):,}')
print(f'selected addresses: {len(selected_addresses):,}')
