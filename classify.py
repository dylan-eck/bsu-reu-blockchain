from untangle import Transaction, untangle
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
print('locating csv files')
csv_file_paths = locate_files('csv_files', "transactions_[0-9]*.csv$")
csv_file_paths = csv_file_paths[0:1]

# --- load transactions ---
print('\nloading transactions')
transactions = []
num_files = len(csv_file_paths)
current_file = 1
for file_path in csv_file_paths:
    transactions += load_transactions_from_csv(file_path) 
    print(f'proccessed file {file_path} ({current_file}/{num_files})')
    current_file += 1

transactions = transactions[:10000]

# --- untangle / classify transactions ---
print('\nclassifying transactions')
num_simple = 0
num_separable = 0
num_ambiguous = 0
num_intract = 0

timing_dict = {
    'simple': [],
    'separable': [],
    'ambiguous': [],
    'intractable': []
}

for transaction in transactions:
    classif_start = perf_counter()

    tx_size_limit = 8
    
    if len(transaction.inputs) == 1 or len(transaction.outputs) == 1:
        num_partitions = 0
        transaction.type = 'simple'
        num_simple += 1

    elif len(transaction.inputs) >= tx_size_limit or len(transaction.outputs) >= tx_size_limit:
        transaction.type = 'intractable'
        num_intract += 1

    else:
        print(f'classifying transaction {transaction.hash}',end='\r')

        partitions = untangle(transaction)
        
        num_partitions = len(partitions)
        if partitions:
            if num_partitions == 1:
                transaction.type = 'separable'
                num_separable += 1
            else:
                transaction.type = 'ambiguous'
                num_ambiguous += 1
        else:
            transaction.type = 'simple'
            num_simple += 1