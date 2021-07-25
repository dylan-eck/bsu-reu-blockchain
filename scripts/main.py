from time import perf_counter
import argparse
import os

from create_transactions_csv import collect_all_transactions, collect_n_transactions
from classify import classify_transactions
from simplify import simplify_transactions
from untangle import untangle_transactions
from construct_graph import construct_graph
from select_addresses import select_addresses
from find_paths import find_paths

def create_scalability_test_input(block_data_directory, data_io_directory):
    n = [100, 1000, 10000, 50000, 100000, 1000000, 5000000, 10000000]
    for num in n:
        collect_n_transactions(block_data_directory, data_io_directory, num)

# command line interface
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input',
                    dest='input_directory',
                    type=str,
                    help='a directory containing the input files'
                    )

parser.add_argument('-o', '-output',
                    dest='output_directory',
                    type=str,
                    help='the directoy where output files are written'
                    )

parser.add_argument('-s', '--scale-test',
                    dest='sc_test',
                    action='store_true'
                    )

# parser.add_argument('-b', '--begin',
#                     dest='begin_day',
#                     type=str,
#                     help='the first day to collect blocks from'
#                     )

# parser.add_argument('-e', '--end',
#                     dest='end_day',
#                     type=str,
#                     help='the last day to collect transactions from')

args = parser.parse_args()

sc_test = args.sc_test

DEFAULT_INPUT_DIRECTORY = '../block_data'
DEFUALT_OUTPUT_DIRECTORY = '../data_out'

if args.input_directory:
    input_directory = args.input_directory
    print(f'using user-specified input directory {input_directory}')
else:
    input_directory = DEFAULT_INPUT_DIRECTORY
    print(f'using default input directory {input_directory}')

if args.output_directory:
    data_io_directory = args.output_directory
    print(f'using user-specified output directory {data_io_directory}')
else:
    data_io_directory = DEFUALT_OUTPUT_DIRECTORY
    print(f'using default output directory {data_io_directory}')
print()

# create input and output directories if they do not already exist
if not os.path.exists(input_directory):
    os.mkdir(input_directory)

if not os.path.exists(data_io_directory):
    os.mkdir(data_io_directory)

fill_char = '-'

# create raw transaction csv files
print(f'{"":{fill_char}<79}')
print('creating raw transaction csv files:')
print(f'{"":{fill_char}<79}\n')

if sc_test:
    create_scalability_test_input(input_directory, data_io_directory)
else:
    collect_all_transactions(input_directory, data_io_directory)

print()

main_start = perf_counter()

# perform initial classification of raw transactions
print(f'{"":{fill_char}<79}')
print('classifying raw transactions and creating new csv files:')
print(f'{"":{fill_char}<79}\n')

classify_transactions(data_io_directory)

print()

# simplify transactions 
print(f'{"":{fill_char}<79}')
print('simplifying transactions:')
print(f'{"":{fill_char}<79}\n')

simplify_transactions(data_io_directory)

print()

# untangle transactions
print(f'{"":{fill_char}<79}')
print('untangling transactions:')
print(f'{"":{fill_char}<79}\n')

untangle_transactions(data_io_directory)

print()

# construct transaction graph for classified raw transactions
print(f'{"":{fill_char}<79}')
print('constructing address graph for address selection:')
print(f'{"":{fill_char}<79}\n')

construct_graph(data_io_directory, 'selection_graph.pickle')

print()

# select addresses to be used for path finding
print(f'{"":{fill_char}<79}')
print('selecting addresses for pathfinding:')
print(f'{"":{fill_char}<79}\n')

select_addresses(data_io_directory, 'selection_graph.pickle')

print()

# construct transaction graph for untangled transactions
print(f'{"":{fill_char}<79}')
print('constructing address graph for pathfinding:')
print(f'{"":{fill_char}<79}\n')

construct_graph(data_io_directory, 'pf_graph.pickle')

print()

# perform pathfinding 
# construct transaction graph for untangled transactions
print(f'{"":{fill_char}<79}')
print('finding paths between addresses:')
print(f'{"":{fill_char}<79}\n')

find_paths(data_io_directory,f'{data_io_directory}/pf_graph.pickle')

print()

# display information about main.py execution
main_end = perf_counter()

print(f'{"":{fill_char}<79}')
print('post excution summary:')
print(f'{"":{fill_char}<79}\n')

print(f'    total execution time: {(main_end - main_start)/60:.2f} minutes')
print()