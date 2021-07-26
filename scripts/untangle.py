from time import perf_counter
from itertools import permutations
import multiprocessing as mp
import os

from transaction import Transaction
from functions import get_file_names, load_transactions_from_csv, func, classify, profile

def untangle_transactions(data_io_directory, file_pattern):
    program_start = perf_counter()

    indent = ''
    if __name__ != '__main__':
        indent = '    '

    num_processes = mp.cpu_count()
    pool = mp.Pool(processes=num_processes)
    print(f'{indent}found {num_processes} available threads\n')

    input_directory = f'{data_io_directory}/simplified_transactions'
    output_directory = f'{data_io_directory}/untangled_transactions'

    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    csv_file_names = get_file_names(input_directory, file_pattern)

    for file in csv_file_names:
        untangle_start = perf_counter()

        print(f'{indent}processing file simplified_transactions/{file}:\n')
        print(f'{indent}    loading transactions... ', end='', flush=True)
        transactions = load_transactions_from_csv(f'{input_directory}/{file}')
        print('done')

        temp = profile(transactions)
        for (key, val) in temp.items():
            print(f'{indent}        {key:>14}: {val:,}')
        print()

        print(f'{indent}    untangling transactions... ', end='', flush=True)
        untangled_txs = pool.map(func, transactions)
        untangled_txs = [item for sublist in untangled_txs for item in sublist]
        print('done')

        print(f'{indent}    reclassifying untangled transactions... ', end='', flush=True)
        for index, tx in enumerate(untangled_txs):
            if tx.type == 'unclassified':
                untangled_txs[index] = classify(tx)
        print('done')

        temp = profile(untangled_txs)
        for (key, val) in temp.items():
            print(f'{indent}        {key:>14}: {val:,}')
        print()
        
        print(f'{indent}    writing new csv file... ', end='', flush=True)
        with open(f'{output_directory}/{file}', 'w') as output_file:
            output_file.write('transaction_hash,num_inputs,input_addresses,input_values,num_outputs,output_addresses,output_values,transaction_fee,transaction_class\n')
            for transaction in untangled_txs:
                output_file.write(transaction.to_csv_string())
        print('done')

        untangle_end = perf_counter()
        print(f'{indent}    finished in {untangle_end - untangle_start:.2f}s\n')

    program_end = perf_counter()
    print(f'{indent}execution finished in {program_end - program_start:.2f}s')

if __name__ == '__main__':
    untangle_transactions('../data_out')