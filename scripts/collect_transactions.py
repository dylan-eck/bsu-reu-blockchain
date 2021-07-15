'''
This script extracts transaction data from block json files retrieved using the blockchain.com api

inputs: block json files placed in the './block_data/' directory

outputs: csv files containing transaction data written to './csv_files/raw_transactions_unclassifed/'
'''
from time import perf_counter
from itertools import chain
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

def get_block_file_paths(day_directory):
    block_file_paths = []

    for (root, dirs, files) in os.walk(day_directory):
        for file in files:
            pattern = re.compile("^[a-z0-9]{64}.json$")
            if pattern.match(file):
                file_path = os.path.join(root, file)
                block_file_paths.append(file_path)

    return block_file_paths

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

        if inputs == [('coinbase','')]:
            continue
        elif inputs and outputs:
            if not None in chain(*inputs) and not None in chain(*outputs):
                transaction = Transaction(hash, inputs, outputs, fee)
                transaction_data.append(transaction)

    return transaction_data

program_start = perf_counter()

csv_headers = 'transaction_hash,input_addresses,input_values,output_addresses,output_values,transaction_fee,classification\n'

output_directory = '../csv_files/raw_transactions_unclassified/'
if not os.path.exists(output_directory):
    os.mkdir(output_directory)

block_data_directory = '../block_data/'
day_directories = get_day_directories(block_data_directory)

for directory in day_directories:
    block_file_paths = get_block_file_paths(block_data_directory + directory)

    print('processing blocks... ', end='\r', flush=True)
    transactions = []
    for file_path in block_file_paths:
        with open(file_path) as input_file:
            block = json.load(input_file)
            transactions += get_tx_data(block)

            block_hash = block['hash']
            print(f'processing blocks... {block_hash}', end='\r', flush=True)
    print(f'{"processing blocks... done":<85}')

    out_file_name =f'{output_directory}{directory}.csv'
    if os.path.exists(out_file_name):
        os.remove(out_file_name)

    print(f'writing file {directory}.csv')
    with open(out_file_name, 'w') as output_file:
        output_file.write(csv_headers)
        for transaction in transactions:
            try:
                output_file.write(f'{transaction.to_csv_string()}')
            except:
                print(f'failed to write transaction {transaction.hash}')

program_end = perf_counter()
execution_time_s = program_end - program_start
print(f'execution finished in {execution_time_s/60:.2f} minutes')