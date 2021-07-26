from time import perf_counter
import argparse
import os

from classify import classify_transactions
from simplify import simplify_transactions
from untangle import untangle_transactions
from construct_graph import construct_graph
from select_addresses import select_addresses
from find_paths import find_paths

if __name__ == '__main__':
    # command line interface
    parser = argparse.ArgumentParser()

    parser.add_argument('-d', '--directory',
                        dest='io_directory',
                        type=str,
                        help='directory where files will be read/written'
                        )

    parser.add_argument('-f', '--fname-con',
                        dest='fname_con',
                        type=str,
                        help='naming convention for target files'
                        )

    parser.add_argument('-c', '-cluster',
                        dest='use_clusters',
                        action='store_true',
                        help='tells the graph constructor that clusters are being used')

    args = parser.parse_args()

    use_clusters = args.use_clusters

    DEFUALT_IO_DIRECTORY = '../data_out'

    if args.io_directory:
        io_directory = args.io_directory
        print(f'using user-specified input directory {io_directory}')
    else:
        io_directory = DEFUALT_IO_DIRECTORY
        print(f'using default input directory {io_directory}')

    # "^[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$"
    # "^tramsactions_[0-9]*.csv$"
    file_naming_convention = args.fname_con

    # create io directory, if it does not already exist
    if not os.path.exists(io_directory):
        os.mkdir(io_directory)

    # used for formatting
    FILL_CHAR_DASH = '-'

    main_start = perf_counter()

    # perform initial classification of raw transactions
    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('classifying raw transactions and creating new csv files:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    classify_transactions(io_directory, file_naming_convention)

    print()

    # simplify transactions 
    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('simplifying transactions:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    simplify_transactions(io_directory, file_naming_convention)

    print()

    # untangle transactions
    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('untangling transactions:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    untangle_transactions(io_directory, file_naming_convention)

    print()

    # construct transaction graph for transaction selection
    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('constructing address graph for address selection:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    construct_graph(f'{io_directory}/raw_transactions_classified', io_directory, file_naming_convention, 'selection_graph.pickle')

    print()

    # select addresses to be used for path finding
    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('selecting addresses for pathfinding:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    select_addresses(io_directory, 'selection_graph.pickle')

    print()

    # construct transaction graph for path finding
    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('constructing address graph for pathfinding:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    construct_graph(f'{io_directory}/untangled_transactions', io_directory, file_naming_convention, 'pf_graph.pickle')

    print()

    # perform pathfinding 
    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('finding paths between addresses:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    find_paths(io_directory, f'{io_directory}/pf_graph.pickle')

    print()

    # display information about main.py execution
    main_end = perf_counter()

    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('post excution summary:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    print(f'    total execution time: {(main_end - main_start)/60:.2f} minutes')
    print()




