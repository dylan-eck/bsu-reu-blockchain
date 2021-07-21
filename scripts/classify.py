'''
This script classifies Bitcoin transactions based on the criteria outlined in "Shared Send Untangling in Bitcoin" 

intputs:    csv files containing transaction data for unclassified transactions

outputs:    csv files containing the input transactions along with their classification
'''
from time import perf_counter
import multiprocessing as mp
import os

from functions import get_file_names, load_transactions_from_csv, classify

def classify_transactions(data_io_directory):
    program_start = perf_counter()

    indent = ''
    if __name__ != '__main__':
        indent = '    '

    num_processes = mp.cpu_count()
    pool = mp.Pool(processes=num_processes)
    print(f'{indent}found {num_processes} available threads\n')

    input_directory = f'{data_io_directory}/raw_transactions_unclassified'
    output_directory = f'{data_io_directory}/raw_transactions_classified'

    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    # --- locate csv files ---
    print(f'{indent}locating csv files... ', end='', flush=True)
    csv_file_names = get_file_names(input_directory, "[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$")
    print('done\n')

    # --- load transactions ---
    for file_name in csv_file_names:
        file_start = perf_counter()

        print(f'{indent}processing file {input_directory}/{file_name}:\n')
        print(f'{indent}    loading transactions... ', end='', flush=True)
        transactions = load_transactions_from_csv(f'{input_directory}/{file_name}')    
        print('done')

        print(f'{indent}    classifying transactions... ', end='', flush=True)
        classified_transactions = pool.map(classify, transactions)
        print('done')

        print(f'{indent}    writing new csv file... ', end='', flush=True)
        with open(f'{output_directory}/{file_name}', 'w') as output_file:
            output_file.write('transaction_hash,num_inputs,input_addresses,input_values,num_outputs,output_addresses,output_values,transaction_fee,transaction_class\n')
            for transaction in classified_transactions:
                output_file.write(transaction.to_csv_string())
        print('done')

        file_end = perf_counter()
        print(f'{indent}    finished in {file_end - file_start:.2f}s\n')

    program_end = perf_counter()
    execution_time = (program_end-program_start)/60/60
    print(f'{indent}execution time: {execution_time:.2f} hours')

if __name__ == '__main__':
    classify_transactions('../data_out')
