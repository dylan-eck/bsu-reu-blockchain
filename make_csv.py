import json
import os
from time import perf_counter

from functions import get_tx_data, get_block_file_paths, write_csv

if not os.path.exists('csv_files'):
    os.mkdir('csv_files')

block_data_directory = 'block_data/'
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

    write_csv(f'csv_files/{file_name}', transactions)
    transactions = []
    file_number += 1

t2 = perf_counter()
print(f'execution time {t2-t1:.2f}s')