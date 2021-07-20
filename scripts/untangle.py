from time import perf_counter
from itertools import permutations
import multiprocessing as mp
import os

from transaction import Transaction
from functions import get_file_names, load_transactions_from_csv, func, classify

if __name__ == '__main__':
    program_start = perf_counter()

    num_processes = mp.cpu_count()
    pool = mp.Pool(processes=2)
    print(f'found {num_processes} available threads\n')

    csv_file_directory = '../csv_files/'
    input_directory = f'{csv_file_directory}simplified_transactions/'
    output_directory = f'{csv_file_directory}untangled_transactions/'

    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    csv_file_names = get_file_names(input_directory, "[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$")

    for file in csv_file_names:
        untangle_start = perf_counter()

        print(f'processing file {input_directory}{file}:')
        print('    loading transactions... ', end='', flush=True)
        transactions = load_transactions_from_csv(f'{input_directory}{file}')
        print('done')

        print('    untangling transactions... ', end='', flush=True)
        untangled_txs = pool.map(func, transactions)
        untangled_txs = [item for sublist in untangled_txs for item in sublist]
        print('done')

        # print(f'    separable txs? {any(tx.type == "separable" for tx in untangled_txs)}')
        
        for tx in untangled_txs:
            classify(tx)
            print(tx.type)

        print(f'    writing new csv file {output_directory}{file}... ', end='', flush=True)
        with open(f'{output_directory}{file}', 'w') as output_file:
            output_file.write('transaction_hash,num_inputs,input_addresses,input_values,num_outputs,output_addresses,output_values,transaction_fee,transaction_class\n')
            for transaction in transactions:
                output_file.write(transaction.to_csv_string())
        print('done')

        untangle_end = perf_counter()
        print(f'    finished in {untangle_end - untangle_start:.2f}s\n')

    program_end = perf_counter()
    print(f'execution finished in {program_end - program_start:.2f}s')