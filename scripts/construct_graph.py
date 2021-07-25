from time import perf_counter
import multiprocessing as mp
import networkx as nx

from functions import get_file_names, load_transactions_from_csv, profile

def get_nodes_and_edges(transaction):
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

    return (nodes, edges)

def construct_graph(data_io_directory, file_pattern, graph_name):
    program_start = perf_counter()

    indent = ''
    if __name__ != '__main__':
        indent = '    '

    threads = mp.cpu_count()
    pool = mp.Pool(processes=threads)
    print(f'{indent}found {threads} available threads')

    print(f'{indent}locating input files... ', end='',flush=True)
    input_directory = f'{data_io_directory}/raw_transactions_classified'
    csv_file_names = get_file_names(input_directory, file_pattern)
    print('done\n')

    graph = nx.Graph()
    
    for file in csv_file_names:
        file_start = perf_counter()

        print(f'{indent}processing file {data_io_directory}/{file}:\n')

        print(f'{indent}    loading transactions... ', end='', flush=True)
        transactions = load_transactions_from_csv(f'{input_directory}/{file}')
        print('done')

        temp = profile(transactions)
        for (key, val) in temp.items():
            print(f'        {key}: {val:,}')
        print()

        print(f'{indent}    collecting graph edges... ', end='', flush=True)
        res = pool.map(get_nodes_and_edges, transactions)
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
    nx.write_gpickle(graph, f'{data_io_directory}/{graph_name}')
    print('done')

    print(f'{indent}added {len(graph.nodes):,} nodes and {len(graph.edges):,} edges to the address graph')

    program_end = perf_counter()
    print(f'{indent}execution finished in {program_end-program_start:.2f}s\n')

if __name__ == '__main__':
    input_directory = '../data_out/raw_transactions_classified'
    construct_graph(input_directory, 'address_graph.pickle')