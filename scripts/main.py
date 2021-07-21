from time import perf_counter
import argparse
import os

from create_transactions_csv import create_transactions_csv
from classify import classify_transactions
from simplify import simplify_transactions
from untangle import untangle_transactions
from construct_graph import construct_graph

main_start = perf_counter()

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

parser.add_argument('-b', '--begin',
                    dest='begin_day',
                    type=str,
                    help='the first day to collect blocks from'
                    )

parser.add_argument('-e', '--end',
                    dest='end_day',
                    type=str,
                    help='the last day to collect transactions from')

args = parser.parse_args()

DEFAULT_INPUT_DIRECTORY = '../block_data'
DEFUALT_OUTPUT_DIRECTORY = '../data_out'

if args.input_directory:
    input_directory = args.input_directory
    print(f'using user-specified input directory {input_directory}')
else:
    input_directory = DEFAULT_INPUT_DIRECTORY
    print(f'using default input directory {input_directory}')

if args.output_directory:
    output_directory = args.output_directory
    print(f'using user-specified output directory {output_directory}')
else:
    output_directory = DEFUALT_OUTPUT_DIRECTORY
    print(f'using default output directory {output_directory}')
print()

# create input and output directories if they do not already exist
if not os.path.exists(input_directory):
    os.mkdir(input_directory)

if not os.path.exists(output_directory):
    os.mkdir(output_directory)

fill_char = '-'

# create raw transaction csv files
print(f'{"":{fill_char}<79}')
print('creating raw transaction csv files:')
print(f'{"":{fill_char}<79}\n')

create_transactions_csv(input_directory, output_directory)

print()

# perform initial classification of raw transactions
print(f'{"":{fill_char}<79}')
print('classifying raw transactions and creating new csv files:')
print(f'{"":{fill_char}<79}\n')

classify_transactions(output_directory)

print()

# simplify transactions 
print(f'{"":{fill_char}<79}')
print('simplifying transactions:')
print(f'{"":{fill_char}<79}\n')

simplify_transactions(output_directory)

print()

# untangle transactions
print(f'{"":{fill_char}<79}')
print('untangling transactions:')
print(f'{"":{fill_char}<79}\n')

untangle_transactions(output_directory)

print()

# construct transaction graph for classified raw transactions
print(f'{"":{fill_char}<79}')
print('constructing address graph for address selection:')
print(f'{"":{fill_char}<79}\n')

construct_graph(output_directory)

print()

# select addresses to be used for path finding

# construct transaction graph for untangled transactions

# perform pathfinding 

# display information about main.py execution
main_end = perf_counter()

print(f'{"":{fill_char}<79}')
print('post excution summary:')
print(f'{"":{fill_char}<79}\n')

print(f'    total execution time: {main_end - main_start:.2f}s')
print()