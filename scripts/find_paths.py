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

    return (addr_pair, path_length)

def find_paths(data_io_directory, graph_path):
    pf_start = perf_counter()

    indent = '' if __name__ == '__main__' else '    '
    
    # load graph
    print(f'{indent}loading graph... ', end='', flush=True)
    pf_graph = nx.read_gpickle(graph_path)
    print('done')

    # load addresses
    print(f'{indent}loading addresses... ', end='', flush=True)
    address_file = f'{data_io_directory}/selected_addresses.csv'
    addresses =  []
    with open(address_file, 'r') as input_file:
        input_file.readline()
        for line in input_file:
            addresses.append(line.strip())
    print('done')

    # find paths
    thread_count = mp.cpu_count()
    pool = mp.Pool(processes=thread_count)

    print(f'{indent}generating address pairs... ', end='', flush=True)
    pairs = combinations(addresses, 2)
    print('done')

    print(f'{indent}finding path lengths... ', end='', flush=True)
    func = partial(get_path_length, graph=pf_graph)
    results = pool.map(func, pairs)
    print('done')

    # create matrix
    print(f'{indent}creating path matrix... ', end='', flush=True)
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
    print(f'{indent}writing path matrix to pickle file... ', end='', flush=True)
    with open(f'{data_io_directory}/path_matrix.pickle', 'wb') as output_file:
        pickle.dump(path_matrix, output_file)
    print('done')

    pf_end = perf_counter()
    print(f'{indent}execution finished in {(pf_end - pf_start)/60:.2f} minutes')

if __name__ == '__main__':
    find_paths('../data_out', '../data_out/pf_graph.pickle')