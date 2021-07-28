from functools import partial
from time import perf_counter
from itertools import combinations
import multiprocessing as mp
import networkx as nx

from functions import get_file_names_regex, load_transactions_from_csv, profile_transactions

def load_clusters(cluster_file_path):
    cluster_dict = {}
    with open(cluster_file_path, 'r') as input_file:
        input_file.readline()
        for line in input_file:
            fields = line.split(',')

            address = fields[0]
            cluster = fields[1].strip()

            if address in cluster_dict:
                print('addresses may not be members of more than one cluster')

            else:
                cluster_dict[address] = cluster
        
    return cluster_dict

def link_cluster_addresses_isolated(cluster_dict, transaction):
    edges = []
    
    input_addresses = [input[0] for input in transaction.inputs]
    output_addresses = [output[0] for output in transaction.outputs]

    # group transaction addresses by cluster
    tx_cluster_dict = {}
    for address in (input_addresses + output_addresses):

        try:
            # check to see if the address is a member of a cluster
            cluster = cluster_dict[address]

        except:
            print(f'{address} not a key in cluster_dict')
            continue

        if cluster in tx_cluster_dict:
            tx_cluster_dict[cluster].append(address)

        else:
            tx_cluster_dict[cluster] = [address]

    # check to see if any of the addresses in the transaction belong to a common cluster
    # if they do, add edges between them
    for key in tx_cluster_dict:
        addresses = tx_cluster_dict[key]
        if len(addresses) > 1:
            for (source, target) in combinations(addresses, 2):
                edges.append((source, target))

    return edges

def get_nodes_and_edges(transaction, with_clusters=False, cluster_dict=None, linked=False):
    nodes = []
    edges = []

    input_addresses = [input[0] for input in transaction.inputs]
    output_addresses = [output[0] for output in transaction.outputs]

    num_inputs = len(input_addresses)
    num_outputs = len(output_addresses)

    # add nodes to graph, keeping track of whether or not they are involved in any many-to-many transactions
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
        if linked:
            # create nodes for clusters and link all addresses in the cluster to the cluster node
            for address in (input_addresses + output_addresses):
                if address in cluster_dict and cluster_dict[address]:
                    edges.append((address, cluster_dict[address]))

        else:
            # directly connect clustered address, but only if they are in a common transaction
            cluster_edges = link_cluster_addresses_isolated(cluster_dict, transaction)
            for edge in cluster_edges:
                edges.append(edge)

    return (nodes, edges)

def construct_graph(input_directory, output_directory, file_pattern, graph_name, with_clusters=False, cluster_file_path='', linked=False):
    global indent # used for output formatting
    
    program_start = perf_counter()

    # multiprocessing is used to speed up execution time
    threads = mp.cpu_count()
    pool = mp.Pool(processes=4)
    print(f'{indent}found {threads} available threads')

    print(f'{indent}locating input files... ', end='',flush=True)
    csv_file_names = get_file_names_regex(input_directory, file_pattern)
    print('done\n')

    global cluster_dict
    if with_clusters:
        print(f'{indent}loading cluster file... ', end='', flush=True)
        cluster_dict = load_clusters(cluster_file_path)
        print('done')
    else:
        cluster_dict = None

    graph = nx.Graph()
    
    for file in csv_file_names:
        file_start = perf_counter()

        print(f'{indent}processing file {input_directory}/{file}:\n')

        print(f'{indent}    loading transactions... ', end='', flush=True)
        transactions = load_transactions_from_csv(f'{input_directory}/{file}')
        print('done')

        tx_summary = profile_transactions(transactions)
        for (key, val) in tx_summary.items():
            print(f'        {key:>14}: {val:,}')
        print()

        print(f'{indent}    collecting graph edges... ', end='', flush=True)
        func = partial(get_nodes_and_edges, with_clusters=with_clusters, cluster_dict=cluster_dict, linked=linked)
        results = pool.map(func, transactions)
        print('done')

        # separate out nodes and eges
        graph_nodes = [sublist[0] for sublist in results]
        graph_edges = [sublist[1] for sublist in results]

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

indent = ''
if __name__ != '__main__':
    indent = '    '