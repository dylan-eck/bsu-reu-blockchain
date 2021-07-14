'''
This script extracts transaction data from block json files retrieved using the blockchain.com api

inputs: block json files placed in the './block_data/' directory

outputs: csv files containing transaction data written to './csv_files/raw_transactions_unclassifed/'
'''
from time import perf_counter
import json
import re
import os

from functions import write_csv

def get_block_file_paths(directory):
    block_file_paths = []

    for (root, dirs, files) in os.walk(directory):
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

        inputs.append([addr, value]) 

    return inputs

def get_outputs(transaction):
    outputs = []

    for output in transaction.get('out'):
        addr = output.get('addr')
        value = output.get('value')

        if addr is not None:
            outputs.append([addr,value])

    return outputs

def get_tx_data(block):
    transactions = block.get('tx')

    transaction_data = []

    for transaction in transactions:
        transaction_hash = transaction.get('hash')
        transaction_fee = transaction.get('fee')

        inputs = get_inputs(transaction)
        outputs = get_outputs(transaction)

        if inputs and outputs:

            input_addr_str = ''
            input_val_str = ''
            for input in inputs:
                if not None in input:
                    input_addr_str += f'{input[0]}:'
                    input_val_str += f'{input[1]}:'
            input_addr_str = input_addr_str[:-1]
            input_val_str = input_val_str[:-1]

            output_addr_str = ''
            output_val_str = ''
            for output in outputs:
                if not None in output:
                    output_addr_str += f'{output[0]}:'
                    output_val_str += f'{output[1]}:'
            output_addr_str = output_addr_str[:-1]
            output_val_str = output_val_str[:-1]

            transaction_data.append([transaction_hash, input_addr_str, input_val_str, output_addr_str, output_val_str, transaction_fee])

    return transaction_data

if not os.path.exists('csv_files'):
    os.mkdir('csv_files')

block_data_directory = '../block_data/'
block_file_paths = get_block_file_paths(block_data_directory)

t1 = perf_counter()

transactions = [['transaction_hash','input_addresses','input_values','output_addresses','output_values','transaction_fee']]

file_number = 0
for file_path in block_file_paths:
    with open(file_path) as input_file:
        block = json.load(input_file)
        block_hash = block.get('hash')
        transactions += get_tx_data(block)
        
        print(f'loaded file {file_path}')

        if(len(transactions) > 1000000):
            file_name = f'transactions_{file_number}.csv'
            if os.path.exists(f'csv_files/{file_name}'):
                os.remove(f'csv_files/{file_name}')
            print(f'writing csv file {file_number}')
            write_csv(f'csv_files/{file_name}', transactions)
            file_number += 1
            transactions = []

print('finished main loop')

if len(transactions) > 0:
    print(f'writing csv file {file_number}')
    file_name = f'transactions_{file_number}.csv'
    if os.path.exists(f'csv_files/{file_name}'):
            os.remove(f'csv_files/{file_name}')

    write_csv(f'csv_files/raw_transactions_unclassifed/{file_name}', transactions)
    transactions = []
    file_number += 1

t2 = perf_counter()
print(f'execution time: {(t2-t1)/60:.2f} minutes')