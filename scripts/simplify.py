from time import perf_counter
import multiprocessing as mp
import os

from functions import classify, get_file_names, load_transactions_from_csv, simplify, profile

def simplify_transactions(data_io_directory, file_pattern):

    indent = ''
    if __name__ != '__main__':
        indent = '    '

    num_processes = mp.cpu_count()
    pool = mp.Pool(processes=num_processes)
    print(f'{indent}found {num_processes} available threads\n')

    input_directory = f'{data_io_directory}/raw_transactions_classified'
    output_directory = f'{data_io_directory}/simplified_transactions'

    if not os.path.exists(output_directory):
        os.mkdir(output_directory)

    csv_file_names = get_file_names(input_directory, file_pattern)

    for file_name in csv_file_names:
        simp_start = perf_counter()

        print(f'{indent}processing file raw_transactions_classified/{file_name}:\n')
        print(f'{indent}    loading transactions... ', end='', flush=True)
        transactions = load_transactions_from_csv(f'{input_directory}/{file_name}')
        print('done')

        temp = profile(transactions)
        for (key, val) in temp.items():
            print(f'        {key:>14}: {val:,}')
        print()

        print(f'{indent}    simplifying transactions... ', end='', flush=True)
        simplified_transactions = pool.map(simplify, transactions)
        print(f'done')

        print(f'{indent}    reclassifying simplifed transactions... ', end='', flush=True)
        for index, tx in enumerate(simplified_transactions):
            if tx.type == 'unclassified':
                simplified_transactions[index] = classify(tx)
        print('done')

        temp = profile(simplified_transactions)
        for (key, val) in temp.items():
            print(f'        {key:>14}: {val:,}')
        print()

        print(f'{indent}    writing new csv file... ', end='', flush=True)
        with open(f'{output_directory}/{file_name}', 'w') as output_file:
            output_file.write('transaction_hash,num_inputs,input_addresses,input_values,num_outputs,output_addresses,output_values,transaction_fee,transaction_class\n')
            for transaction in simplified_transactions:
                output_file.write(transaction.to_csv_string())
        print('done')

        simp_end = perf_counter()
        print(f'{indent}    finished in {simp_end - simp_start:.2f}s')

if __name__ == '__main__':
    simplify_transactions('../data_out')
