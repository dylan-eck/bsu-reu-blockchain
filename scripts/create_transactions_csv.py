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

def collect_n_transactions(block_data_directory, data_io_directory, num):
    global indent
    print(f'{indent}collecting {num:,} transactions... ', end='', flush=True)

    input_directory = block_data_directory
    day_directories = get_day_directories(input_directory)

    output_directory = f'{data_io_directory}/raw_transactions_unclassified'
    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    transactions = []
    for directory_name in day_directories:
        block_files = get_block_files(f'{input_directory}/{directory_name}')

        for file_name in block_files:
            with open(f'{input_directory}/{directory_name}/{file_name}') as input_file:
                block = json.load(input_file)
                transactions += get_tx_data(block)
                
                if len(transactions) > num:
                    print('done')
                    while len(transactions) > num:
                        transactions.pop()

                    out_file_name =f'{output_directory}/{num}_txs.csv'
                    with open(out_file_name, 'w') as output_file:
                        csv_headers = 'transaction_hash,input_addresses,input_values,output_addresses,output_values,transaction_fee,classification\n'
                        output_file.write(csv_headers)
                        for transaction in transactions:
                            output_file.write(transaction.to_csv_string())
                    
                    return
    print('done')

def collect_all_transactions(block_data_directory, data_io_directory):
    global indent
    
    program_start = perf_counter()

    input_directory = block_data_directory
    day_directories = get_day_directories(input_directory)

    output_directory = f'{data_io_directory}/raw_transactions_unclassified'
    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    for directory_name in day_directories:
        day_start = perf_counter()

        print(f'{indent}processing day {directory_name}:\n')

        print(f'{indent}    locating block files... ', end='', flush=True)
        block_files = get_block_files(f'{input_directory}/{directory_name}')
        print('done')

        transactions = []
        for file_name in block_files:
            print(f'{indent}    processing blocks... {file_name[:-4]}', end='\r', flush=True)
            with open(f'{input_directory}/{directory_name}/{file_name}') as input_file:
                block = json.load(input_file)
                transactions += get_tx_data(block)

        print(f'{f"{indent}    processing blocks... done":<100}')

        print(f'{indent}    writing new csv file... ', end='', flush=True)
        out_file_name =f'{output_directory}/{directory_name}.csv'
        with open(out_file_name, 'w') as output_file:
            csv_headers = 'transaction_hash,input_addresses,input_values,output_addresses,output_values,transaction_fee,classification\n'
            output_file.write(csv_headers)
            for transaction in transactions:
                output_file.write(transaction.to_csv_string())
        print('done')

        day_end = perf_counter()
        print(f'{indent}    finished in {day_end - day_start:.2f}s\n')

    program_end = perf_counter()
    execution_time_s = program_end - program_start
    print(f'{indent}execution finished in {execution_time_s/60:.2f} minutes')

indent = ''
if __name__ != '__main__':
    indent = '    '

if __name__ == '__main__':
    collect_all_transactions('../block_data', '../data_out')