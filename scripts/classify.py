'''
This script classifies Bitcoin transactions based on the criteria outlined in "Shared Send Untangling in Bitcoin" 

intputs:    csv files containing transaction data for unclassified transactions

outputs:    csv files containing the input transactions along with their classification
'''

from time import perf_counter
import os

from untangle import untangle
from transaction import Transaction
from functions import get_file_names, load_transactions_from_csv
    
program_start = perf_counter()

csv_file_directory = '../csv_files/'
input_directory = f'{csv_file_directory}raw_transactions_unclassified/'
output_directory = f'{csv_file_directory}raw_transactions_classified/'

# --- locate csv files ---
print('locating csv files... ', end='', flush=True)
csv_file_names = get_file_names(input_directory, "[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$")
print('done')

# --- load transactions ---
for file_name in csv_file_names:
    transactions = load_transactions_from_csv(f'{input_directory}{file_name}')    

    total_transactions = len(transactions)
    current_transaction = 1
    for transaction in transactions:
        if transaction.type == 'unclassified':
            print(f'classifying transactions... {transaction.hash} ({current_transaction:,}/{total_transactions:,})', end='\r', flush=True)
            current_transaction += 1
            classif_start = perf_counter()

            tx_size_limit = 8
            if len(transaction.inputs) == 1 or len(transaction.outputs) == 1:
                transaction.type = 'simple'

            elif len(transaction.inputs) >= tx_size_limit or len(transaction.outputs) >= tx_size_limit:
                transaction.type = 'intractable'

            else:
                partitions = untangle(transaction)
                num_partitions = len(partitions)
                if partitions:
                    if num_partitions == 1:
                        transaction.type = 'separable'
                    else:
                        transaction.type = 'ambiguous'
                else:
                    transaction.type = 'simple'

    print(f'{f"classifying transactions... ({current_transaction-1:,}/{total_transactions:,})":<100}')

    print(f'writing new csv file... {output_directory}{file_name}', end='\r', flush=True)
    with open(f'{output_directory}{file_name}', 'w') as output_file:
        output_file.write('transaction_hash,num_inputs,input_addresses,input_values,num_outputs,output_addresses,output_values,transaction_fee,transaction_class\n')
        for transaction in transactions:
            output_file.write(transaction.to_csv_string())
    print(f'{"writing new csv file... done":<100}')

program_end = perf_counter()
execution_time = (program_end-program_start)/60/60
print(f'execution time: {execution_time:.2f} hours')