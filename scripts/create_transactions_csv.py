'''
This script extracts transaction data from block json files retrieved using the blockchain.com api

inputs: block json files placed in the './block_data/' directory

outputs: csv files containing transaction data written to './csv_files/raw_transactions_unclassifed/'
'''
from time import perf_counter
from itertools import chain
import argparse
import json
import re
import os

from transaction import Transaction

def get_day_directories(block_data_directory):
    day_directories = []

    for (root, dirs, files) in os.walk(block_data_directory):
        pattern = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
        for dir in dirs:
            if pattern.match(dir):
                day_directories.append(dir)

    return day_directories

def get_block_files(day_directory):
    block_files = []

    for (root, dirs, files) in os.walk(day_directory):
        for file in files:
            pattern = re.compile("^[a-z0-9]{64}.json$")
            if pattern.match(file):
                block_files.append(file)

    return block_files

def get_inputs(transaction):
    inputs = []

    for input in transaction.get('inputs'):
        prev_out = input.get('prev_out')
        
        if prev_out is not None:
            addr = prev_out.get('addr')
            value = prev_out.get('value')
        else:
            addr = 'coinbase'
            value = ''

        inputs.append((addr, value)) 

    return inputs

def get_outputs(transaction):
    outputs = []

    for output in transaction.get('out'):
        addr = output.get('addr')
        value = output.get('value')

        if addr is not None:
            outputs.append((addr,value))

    return outputs

def get_tx_data(block):
    transactions = block.get('tx')

    transaction_data = []

    for transaction in transactions:
        hash = transaction.get('hash')
        fee = transaction.get('fee')

        inputs = get_inputs(transaction)
        outputs = get_outputs(transaction)

        if inputs and outputs:
            if not None in chain(*inputs) and not None in chain(*outputs):
                transaction = Transaction(hash, inputs, outputs, fee)
                transaction_data.append(transaction)

    return transaction_data

def collect_transactions_by_chunk(input_directory, output_directory, chunk_size, num_chunks=None):
    input_directory = input_directory
    day_directories = get_day_directories(input_directory)

    output_directory = f'{output_directory}/raw_transactions_unclassified'
    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    transactions = []
    chunk_num = 1
    print(f'    collecting chunk {chunk_num}... ', end='', flush=True)
    for directory_name in day_directories:
        block_files = get_block_files(f'{input_directory}/{directory_name}')

        for file_name in block_files:
            with open(f'{input_directory}/{directory_name}/{file_name}') as input_file:
                block = json.load(input_file)
                transactions += get_tx_data(block)
                
                if len(transactions) > chunk_size:
                    
                    extra_transactions = []
                    while len(transactions) > chunk_size:
                        extra_transactions.append(transactions.pop())

                    out_file_name =f'{output_directory}/transactions_{chunk_num}.csv'
                    with open(out_file_name, 'w') as output_file:
                        csv_headers = 'transaction_hash,input_addresses,input_values,output_addresses,output_values,transaction_fee,classification\n'
                        output_file.write(csv_headers)
                        for transaction in transactions:
                            output_file.write(transaction.to_csv_string())

                    print('done')
                    if num_chunks and chunk_num == num_chunks:
                        return
                    transactions = extra_transactions
                    chunk_num += 1
                    print(f'    collecting chunk {chunk_num}... ', end='', flush=True)

def collect_transactions_by_day(input_directory, output_directory):
    day_directories = get_day_directories(input_directory)

    if not os.path.exists(f'{output_directory}/raw_transactions_unclassified'):
        os.mkdir(f'{output_directory}/raw_transactions_unclassified')

    for directory_name in day_directories:
        day_start = perf_counter()

        print(f'    processing day {directory_name}:\n')

        print(f'        locating block files... ', end='', flush=True)
        block_files = get_block_files(f'{input_directory}/{directory_name}')
        print('done')

        transactions = []
        for file_name in block_files:
            print(f'        processing blocks... {file_name[:8]}...{file_name[-13:-5]}', end='\r', flush=True)
            with open(f'{input_directory}/{directory_name}/{file_name}') as input_file:
                block = json.load(input_file)
                transactions += get_tx_data(block)

        print(f'{f"        processing blocks... done":<79}')

        print(f'        writing new csv file... ', end='', flush=True)
        out_file_name =f'{output_directory}/raw_transactions_unclassified/{directory_name}.csv'
        with open(out_file_name, 'w') as output_file:
            csv_headers = 'transaction_hash,input_addresses,input_values,output_addresses,output_values,transaction_fee,classification\n'
            output_file.write(csv_headers)
            for transaction in transactions:
                output_file.write(transaction.to_csv_string())
        print('done')

        day_end = perf_counter()
        print(f'        finished in {day_end - day_start:.2f}s\n')

FILL_CHAR_DASH = '-'

if __name__ == '__main__':
    # command line interface
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--in-dir',
                        dest='input_directory',
                        type=str,
                        help='a directory containing the input json files'
                        )

    parser.add_argument('-o', '-out-dir',
                        dest='output_directory',
                        type=str,
                        help='the directoy where output csv files will be written'
                        )

    parser.add_argument('-d', '-days',
                        dest='group_by_days',
                        action='store_true',
                        help='ouput csv files will be broken up by date'
                        )

    parser.add_argument('-c', '-chunks',
                        dest='group_by_chunks',
                        type=int,
                        nargs=2,
                        help='ouput csv files will be broken up into equal size chunks'
                        )
    
    args = parser.parse_args()

    print(f'{"":{FILL_CHAR_DASH}<79}')
    print('creating raw transaction csv files:')
    print(f'{"":{FILL_CHAR_DASH}<79}\n')

    DEFAULT_INPUT_DIRECTORY = '../block_data'
    DEFUALT_OUTPUT_DIRECTORY = '../data_out'

    if args.input_directory:
        input_directory = args.input_directory
        print(f'    using user-specified input directory {input_directory}')
    else:
        input_directory = DEFAULT_INPUT_DIRECTORY
        print(f'    using default input directory {input_directory}')

    if args.output_directory:
        output_directory = args.output_directory
        print(f'    using user-specified output directory {output_directory}')
    else:
        output_directory = DEFUALT_OUTPUT_DIRECTORY
        print(f'    using default output directory {output_directory}')

    collection_start = perf_counter()

    if args.group_by_days:
        print('    grouping transactions by date\n')
        collect_transactions_by_day(input_directory, output_directory)
    
    elif args.group_by_chunks:
        chunk_size = args.group_by_chunks[0]
        num_chunks = args.group_by_chunks[1]
        print(f'    grouping transactions into {num_chunks} batches of size {chunk_size:,}\n')
        collect_transactions_by_chunk(input_directory, output_directory, chunk_size, num_chunks)
        print()

    collection_end = perf_counter()
    execution_time_s = collection_end - collection_start
    print(f'    finished creating csv files in {execution_time_s/60:.2f} minutes\n')