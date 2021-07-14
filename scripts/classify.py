from untangle import Transaction, remove_small_outputs, untangle, consolodate_same_addresses, remove_small_inputs
from time import perf_counter
import os
import re

def transactions_from_partitions(transaction, partitions):
    transactions = []

    hash = transaction.hash
    fee = transaction.fee

    for partition in partitions:
        inputs = partition[0]
        outputs = partition[1]
        partition_size = len(inputs)

        for i in range(partition_size):
            transaction = Transaction(hash,inputs[i],outputs[i],fee)
            transactions.append(transaction)

    return transactions

def locate_files(direcotry, pattern):
    file_paths = []
    for (root, dirs, files) in os.walk(direcotry):
        for file in files:
            pattern = re.compile(pattern)
            if pattern.match(file):
                file_path = os.path.join(root, file)
                file_paths.append(file_path)
    return file_paths

def load_transactions_from_csv(csv_file_path):
    transactions = []

    with open(csv_file_path, 'r') as input_file:
        input_file.readline()
        for line in input_file:
            transaction = Transaction()
            transaction.from_csv_string(line)

            if not transaction.is_coinbase():
                transactions.append(transaction)

    return transactions
    
program_start = perf_counter()

# --- locate csv files ---
print('locating csv files... ', end='', flush=True)
csv_file_paths = locate_files('csv_files/T', "transactions_[0-9]*.csv$")
print('done')

# --- load transactions ---
num_files = len(csv_file_paths)
current_file = 1
for file_path in csv_file_paths:
    transactions = load_transactions_from_csv(file_path)    

    # --- untangle / classify transactions ---
    txs_to_remove = []

    total_transactions = len(transactions)
    current_transaction = 1
    for transaction in transactions:
        print(f'classifying transactions... {transaction.hash} ({current_transaction:,}/{total_transactions:,})', end='\r')
        current_transaction += 1
        classif_start = perf_counter()

        transaction = consolodate_same_addresses(transaction)
        transaction = remove_small_inputs(transaction)

        tx_size_limit = 8
        if len(transaction.inputs) == 1 or len(transaction.outputs) == 1:
            num_partitions = 0
            transaction.type = 'simple'

        elif len(transaction.inputs) >= tx_size_limit or len(transaction.outputs) >= tx_size_limit:
            transaction.type = 'intractable'

        else:
            transaction = remove_small_outputs(transaction)
            partitions = untangle(transaction)
            num_partitions = len(partitions)
            if partitions:
                if num_partitions == 1:
                    transaction.type = 'separable'
                else:
                    transaction.type = 'ambiguous'
            else:
                transaction.type = 'simple'

        if transaction.type == 'ambiguous' or transaction.type == 'intractable':
            txs_to_remove.append(transaction)
            
        elif transaction.type == 'separable':
            transaction_index = transactions.index(transaction)

            sub_transactions = transactions_from_partitions(transaction, partitions)
            for sub_transaction in sub_transactions:
                transactions.insert(transaction_index + 1, sub_transaction)
                total_transactions += 1

            txs_to_remove.append(transaction)

    print(f'classifying transactions... ({current_transaction-1:,}/{total_transactions:,}) {len(transactions)}                                                       ')

    transactions = [tx for tx in transactions if not tx in txs_to_remove]                                                                

    print(f'writing new csv file... csv_files/S_0/transactions_{current_file-1}.csv', end='\r', flush=True)
    with open(f'csv_files/S_0/transactions_{current_file-1}.csv', 'w') as output_file:
        output_file.write('transaction_hash,num_inputs,input_addresses,input_values,num_outputs,output_addresses,output_values,transaction_fee,transaction_class\n')
        for transaction in transactions:
            output_file.write(transaction.to_csv_string())
    print(f'writing new csv file... done                                               ')
    current_file += 1

program_end = perf_counter()
execution_time = (program_end-program_start)/60/60
print(f'execution time: {execution_time:.2f} hours')