from time import perf_counter
import networkx as nx
import random

def select_addresses(data_io_directory, graph_path):
    start = perf_counter()
    
    indent = ''
    if __name__ != '__main__':
        indent = '    '

    print(f'{indent}loading graph... ', end='', flush=True)
    address_graph = nx.read_gpickle(graph_path)
    print('done')

    print(f'{indent}selecting addresses... ', end='', flush=True)
    mtm_addrs = []
    selected_addresses = []

    for node in address_graph.nodes:
        if address_graph.nodes[node]['in_mtm']:
            mtm_addrs.append(node)

    for address in mtm_addrs:
        if len(address_graph.adj[address]) > 0:
            for neighbor in address_graph.adj[address]: 
                if not address_graph.nodes[neighbor]['in_mtm']:
                    selected_addresses.append(neighbor)

    n = 2000
    selected_addresses = random.sample(selected_addresses, n)
    print('done')

    print(f'{indent}writing csv file... ', end='', flush=True)
    output_directory = data_io_directory
    with open(f'{output_directory}/selected_addresses.csv', 'w') as output_file:
        output_file.write('address')
        for address in selected_addresses:
            output_file.write(f'\n{address}')
    print('done')

    end = perf_counter()
    print(f'{indent}finished in {end - start:.2f}s\n')

if __name__ == '__main__':
    d = '../data_out'
    select_addresses(d,f'{d}/address_graph.pickle')