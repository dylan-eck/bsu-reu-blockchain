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

    parser.add_argument(
        '-d',
        '--directory',
        dest='io_directory',
        type=str,
        help='directory where files will be read/written')

    parser.add_argument(
        '-f',
        '--file-regex',
        dest='file_regex',
        type=str,
        help='regex pattern used to locate input files')

    parser.add_argument(
        '-c',
        '-cluster',
        dest='clusters',
        type=str,
        nargs=1,
        help='use clusterd from the specified file during graph construction')

    parser.add_argument(
        '-s',
        '-select-addrs',
        dest='select_addrs',
        action='store_true',
        help='perform address selection'
    )

    args = parser.parse_args()

    # process command line arguments
    DEFUALT_IO_DIRECTORY = '../data_out'
    if args.io_directory:
        io_directory = args.io_directory
        print(f'using user-specified input directory {io_directory}')
    else:
        io_directory = DEFUALT_IO_DIRECTORY
        print(f'using default input directory {io_directory}')

    if not os.path.exists(io_directory):
        os.mkdir(io_directory)

    file_regex_pattern = args.file_regex

    if args.clusters:
        clusters = args.clusters[0]
    else:
        clusters = None

    FILL_CHAR_DASH = '-'  # used for ouput formatting

    main_start = perf_counter()

    # perform initial classification of raw transactions
    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('classifying raw transactions and creating new csv files:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    classify_transactions(io_directory, file_regex_pattern)

    # simplify transactions
    print(f'\n{"":{FILL_CHAR_DASH}<79}')
    print('simplifying transactions:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    simplify_transactions(io_directory, file_regex_pattern)

    # untangle transactions
    print(f'\n{"":{FILL_CHAR_DASH}<79}')
    print('untangling transactions:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    untangle_transactions(io_directory, file_regex_pattern)

    if args.select_addrs:
        # construct address graph for transaction selection
        print(f'\n{"":{FILL_CHAR_DASH}<79}')
        print('constructing address graph for address selection:')
        print(f'{"":{FILL_CHAR_DASH}<79}\n')

        construct_graph(
            f'{io_directory}/raw_transactions_classified',
            io_directory,
            file_regex_pattern,
            'selection_graph.pickle')

        # select addresses to be used for path finding
        print(f'\n{"":{FILL_CHAR_DASH}<79}')
        print('selecting addresses for pathfinding:')
        print(f'{"":{FILL_CHAR_DASH}<79}\n')

        select_addresses(io_directory, 'selection_graph.pickle')

    else:
        print(f'\n{"":{FILL_CHAR_DASH}<79}')
        print('selecting addresses for pathfinding:')
        print(f'{"":{FILL_CHAR_DASH}<79}\n')
        print('    using pre-selected addresses')

    # construct address graph for path finding
    print(f'\n{"":{FILL_CHAR_DASH}<79}')
    print('constructing address graph for pathfinding:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    if clusters:
        construct_graph(
            f'{io_directory}/untangled_transactions',
            io_directory,
            file_regex_pattern,
            'pf_graph.pickle',
            with_clusters=True,
            cluster_file_path=clusters,
            linked=True)

    else:
        construct_graph(
            f'{io_directory}/untangled_transactions',
            io_directory,
            file_regex_pattern,
            'pf_graph.pickle')

    # perform pathfinding
    print(f'\n{"":{FILL_CHAR_DASH}<79}')
    print('finding paths between addresses:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    find_paths(io_directory, f'{io_directory}/pf_graph.pickle')

    # display information about main.py execution
    main_end = perf_counter()

    print(f'\n{"":{FILL_CHAR_DASH}<79}')
    print('post excution summary:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    print(
        f'    total execution time: {(main_end - main_start)/60:.2f} minutes\n')
