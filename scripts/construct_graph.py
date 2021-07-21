from time import perf_counter
import multiprocessing as mp
import networkx as nx

from functions import get_file_names, load_transactions_from_csv, profile

def get_edges(transaction):
    input_addresses = [input[0] for input in transaction.inputs]
    output_addresses = [output[0] for output in transaction.outputs]

    # only add egdes for simple transactions 
    # (the untangled data should not contain any separable transactions)
    edges = []
    if transaction.type == 'simple':
        for i in input_addresses:
            for j in output_addresses:
                edges.append((i,j))
    return edges

if __name__ == '__main__':
    program_start = perf_counter()

    threads = mp.cpu_count()
    pool = mp.Pool(processes=threads)
    print(f'found {threads} available threads')

    print('locating input files... ', end='',flush=True)
    csv_file_directory = '../csv_files/'
    input_directory = f'{csv_file_directory}untangled_transactions/'
    csv_file_names = get_file_names(input_directory, "[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$")
    print('done\n')

    address_graph = nx.DiGraph()
    
    for file in csv_file_names:
        file_start = perf_counter()

        print(f'processing file {input_directory}{file}:\n')

        print('    loading transactions... ', end='', flush=True)
        transactions = load_transactions_from_csv(f'{input_directory}{file}')
        print('done')

        temp = profile(transactions)
        for (key, val) in temp.items():
            print(f'        {key}: {val:,}')
        print()

        print('    collecting graph edges... ', end='', flush=True)
        graph_edges = pool.map(get_edges, transactions)
        print('done')

        print('    adding edges to graph... ', end='', flush=True)
        for edge_list in graph_edges:
            address_graph.add_edges_from(edge_list)
        print('done')

        file_end = perf_counter()
        print(f'    finished in {file_end - file_start:.2f}s\n')
        
    print('writing graph to pickle file... ', end='', flush=True)
    nx.write_gpickle(address_graph, '../data_out/address_graph.pickle')
    print('done')

    print(f'added {len(address_graph.nodes):,} nodes and {len(address_graph.edges):,} edges to the address graph')

    program_end = perf_counter()
    print(f'execution finished in {program_end-program_start:.2f}s')