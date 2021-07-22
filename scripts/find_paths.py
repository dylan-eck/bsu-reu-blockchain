from time import perf_counter
from functools import partial
from itertools import combinations
import networkx as nx
import multiprocessing as mp
import pickle

def get_path_length(addr_pair, graph):
    path_length = 0

    source = addr_pair[0]
    target = addr_pair[1]
    
    try:
        path = nx.bidirectional_shortest_path(graph, source, target)
        path_length = len(path)
        
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        path_length = -1

    print(f'finding path lengths... {source[:8]}... -> {target[:8]}... : {path_length:>3}', end='\r', flush=True)
    return (addr_pair, path_length)

pf_start = perf_counter()

# load graph
print('loading graph... ', end='', flush=True)
pf_graph = nx.read_gpickle('../data_out/pf_graph.pickle')
print('done')

# load addresses
print('loading addresses... ', end='', flush=True)
address_file = '../data_out/selected_addresses.csv'
addresses =  []
with open(address_file, 'r') as input_file:
    input_file.readline()
    for line in input_file:
        addresses.append(line.strip())
print('done')

# find paths
thread_count = mp.cpu_count()
pool = mp.Pool(processes=thread_count)

print('generating address pairs... ', end='', flush=True)
pairs = combinations(addresses, 2)
print('done')

func = partial(get_path_length, graph=pf_graph)
results = pool.map(func, pairs)
print(f'{"finding path lengths... done":<79}')

# create matrix
print('creating path matrix... ', end='', flush=True)
path_matrix = {}
for result in results:
    source = result[0][0]
    target = result[0][1]
    path_length = result[1]

    if not source in path_matrix:
        pl_dict = {}
        pl_dict[target] = path_length

        path_matrix[source] = pl_dict

    else:
        path_matrix[source][target] = path_length
print('done')

# write matrix to file
print('writing path matrix to pickle file... ', end='', flush=True)
with open('../data_out/path_matrix.pickle', 'wb') as output_file:
    pickle.dump(path_matrix, output_file)
print('done')

pf_end = perf_counter()
print(f'execution finished in {(pf_end - pf_start)/60:.2f} minutes')