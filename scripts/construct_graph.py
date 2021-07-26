from functools import partial
from time import perf_counter
from itertools import combinations
import multiprocessing as mp
import networkx as nx

from functions import get_file_names, load_transactions_from_csv, profile

def link_cluster_addresses(cluster_dict, transaction):
    edges = []
    
    input_addresses = [input[0] for input in transaction.inputs]
    output_addresses = [output[0] for output in transaction.outputs]

    # group transaction addresses by cluster
    tx_cluster_dict = {}
    for address in (input_addresses + output_addresses):

        try:
            cluster = cluster_dict[address]

        except:
            # print(f'{address} not a key in cluster_dict')
            return edges

        if cluster in tx_cluster_dict:
            tx_cluster_dict[cluster].append(address)

        else:
            tx_cluster_dict[cluster] = [address]

    # check to see if any of the addresses in the graph belong to a common cluster
    for key in tx_cluster_dict:
        addresses = tx_cluster_dict[key]
        if len(addresses) > 1:
            for (source, target) in combinations(addresses, 2):
                edges.append((source, target))

    return edges

def get_nodes_and_edges(transaction, with_clusters=False, cluster_dict=None):
    nodes = []
    edges = []

    input_addresses = [input[0] for input in transaction.inputs]
    output_addresses = [output[0] for output in transaction.outputs]

    num_inputs = len(input_addresses)
    num_outputs = len(output_addresses)

    if num_inputs == 1 or num_outputs == 1:
        for i in input_addresses:
            for j in output_addresses:

                if not i in nodes:
                    nodes.append((i, {'in_mtm': False}))

                if not j in nodes:
                    nodes.append((j, {'in_mtm': False}))

                edges.append((i,j))

    else:
        for i in input_addresses:
            if not i in nodes:
                nodes.append((i, {'in_mtm': True}))

            else:
                nodes[nodes.index(i)] = (i, {'in_mtm': True})

        for j in output_addresses:
            if not j in nodes:
                nodes.append((j, {'in_mtm': True}))

            else:
                nodes[nodes.index(j)] = (j, {'in_mtm': True})

    if with_clusters:
        cluster_edges = link_cluster_addresses(cluster_dict, transaction)
        for edge in cluster_edges:
            edges.append(edge)

    return (nodes, edges)

def construct_graph(input_directory, output_directory, file_pattern, graph_name, with_clusters=False, cluster_file_path=''):
    program_start = perf_counter()

    indent = ''
    if __name__ != '__main__':
        indent = '    '

    threads = mp.cpu_count()
    pool = mp.Pool(processes=4)
    print(f'{indent}found {threads} available threads')

    print(f'{indent}locating input files... ', end='',flush=True)
    csv_file_names = get_file_names(input_directory, file_pattern)

    global cluster_dict
    if with_clusters:
        cluster_dict = load_clusters(cluster_file_path)
    else:
        cluster_dict = None

    print('done\n')

    graph = nx.Graph()
    
    for file in csv_file_names:
        file_start = perf_counter()

        print(f'{indent}processing file raw_transactions_classified/{file}:\n')

        print(f'{indent}    loading transactions... ', end='', flush=True)
        transactions = load_transactions_from_csv(f'{input_directory}/{file}')
        print('done')

        temp = profile(transactions)
        for (key, val) in temp.items():
            print(f'        {key:>14}: {val:,}')
        print()

        print(f'{indent}    collecting graph edges... ', end='', flush=True)
        func = partial(get_nodes_and_edges, with_clusters=with_clusters, cluster_dict=cluster_dict)

        res = pool.map(func, transactions)
        print('done')

        graph_nodes = [sl[0] for sl in res]
        graph_edges = [sl[1] for sl in res]

        print(f'{indent}    adding edges to graph... ', end='', flush=True)

        for node_list in graph_nodes:
            graph.add_nodes_from(node_list)

        for edge_list in graph_edges:
            graph.add_edges_from(edge_list)
        print('done')

        file_end = perf_counter()
        print(f'{indent}    finished in {file_end - file_start:.2f}s\n')
        
    print(f'{indent}writing graph to pickle file... ', end='', flush=True)
    nx.write_gpickle(graph, f'{output_directory}/{graph_name}')
    print('done')

    print(f'{indent}added {len(graph.nodes):,} nodes and {len(graph.edges):,} edges to the address graph')

    program_end = perf_counter()
    print(f'{indent}execution finished in {program_end-program_start:.2f}s\n')

def load_clusters(cluster_file_path):
    cluster_dict = {}
    with open(cluster_file_path, 'r') as input_file:
        input_file.readline()
        for line in input_file:
            fields = line.split(',')

            address = fields[0]
            cluster = fields[1].strip()

            cluster_dict[address] = cluster
        
    return cluster_dict

if __name__ == '__main__':
    input_directory = '../2021-06-03_2021-06-09'
    construct_graph(input_directory, "^[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$", 'address_graph.pickle', with_clusters=True, cluster_file_path=f'{input_directory}/clusters_2021-06-03-2021-06-09.csv')