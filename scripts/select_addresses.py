from time import perf_counter
import networkx as nx
import random
import os

def select_addresses(data_io_directory, graph_name):
    global indent # used for output formatting

    start = perf_counter()

    # if a selected addresses file already exists, do not create a new one
    if os.path.exists(f'{data_io_directory}/selected_addresses.csv'):
        print(f'{indent}using preselected addresses')
        return

    print(f'{indent}loading graph... ', end='', flush=True)
    address_graph = nx.read_gpickle(f'{data_io_directory}/{graph_name}')
    print('done')

    print(f'{indent}selecting addresses... ', end='', flush=True)
    mtm_addrs = []
    selected_addresses = []

    # find all addresses involved in at least one many-to-many transaction
    for node in address_graph.nodes:
        if address_graph.nodes[node]['in_mtm']:
            mtm_addrs.append(node)

    # using the above set of addresess, find all neighbors that are not involved in any many-to-many transactions
    for address in mtm_addrs:
        if len(address_graph.adj[address]) > 0:
            for neighbor in address_graph.adj[address]: 
                if not address_graph.nodes[neighbor]['in_mtm']:
                    selected_addresses.append(neighbor)

    # limit the number of selected addresses to 2000
    n = 2000
    selected_addresses = random.sample(selected_addresses, n)
    print('done')

    print(f'{indent}writing csv file... ', end='', flush=True)
    with open(f'{data_io_directory}/selected_addresses.csv', 'w') as output_file:

        output_file.write('address')
        
        for address in selected_addresses:
            output_file.write(f'\n{address}')
    print('done')

    end = perf_counter()
    print(f'{indent}finished in {end - start:.2f}s\n')

indent = ''
if __name__ != '__main__':
    indent = '    '

if __name__ == '__main__':
    d = '../data_out'
    select_addresses(d,f'{d}/address_graph.pickle')