from time import perf_counter
from functools import partial
from itertools import combinations
import networkx as nx
import multiprocessing as mp

def get_path_length(addr_pair, graph):
    path_length = 0

    source = addr_pair[0]
    target = addr_pair[1]
    
    try:
        path = nx.bidirectional_shortest_path(graph, source, target)
        path_length = len(path)
        
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        path_length = -1

    print(f'\rfinding path lengths... {source[:8]}... -> {target[:8]}... : {path_length:>3}', end='', flush=True)
    return (addr_pair, path_length)

pf_start = perf_counter()

# load graph
pf_graph = nx.read_gpickle('../data_out/pf_graph.pickle')

# load addresses
address_file = '../data_out/selected_addresses.csv'
addresses =  []
with open(address_file, 'r') as input_file:
    input_file.readline()
    for line in input_file:
        addresses.append(line.strip())

# find paths
thread_count = mp.cpu_count()
pool = mp.Pool(processes=6)

print('generating address pairs... ', end='', flush=True)
pairs = combinations(addresses, 2)
print('done')

print('finding path lengths... ', end='', flush=True)
func = partial(get_path_length, graph=pf_graph)
path_lengths = pool.map(func, pairs)
print('\rfinding path lengths... done')

# create matrix

# write matrix to file

pf_end = perf_counter()
print(f'execution finished in {(pf_end - pf_start)/60:.2f} minutes')