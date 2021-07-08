from untangle import untangle
from time import perf_counter
import os
import re

def read_transaction(line):
    line = line.split(',')
    transaction_hash = line[0]
    input_addresses = line[1].split(':')
    if input_addresses == ['coinbase']:
        return

    input_values = [int(x) for x in line[2].split(':')]
    output_addresses = line[3].split(':')
    output_values = [int(x) for x in line[4].split(':')]
    transaction_fee = int(line[5])

    inputs = list(zip(input_addresses, input_values))
    outputs = list(zip(output_addresses, output_values))

    return [transaction_hash, inputs, outputs, transaction_fee]

def is_large(transaction):
    size_limit = 10
    if len(transaction[1]) > size_limit or len(transaction[2]) > size_limit:
        return True
    else:
        return False

program_start = perf_counter()

# --- locate csv files ---
print('locating csv files')
csv_file_paths = []
for (root, dirs, files) in os.walk('csv_files'):
        for file in files:
            pattern = re.compile("transactions_[0-9]*.csv$")
            if pattern.match(file):
                file_path = os.path.join(root, file)
                csv_file_paths.append(file_path)
csv_file_paths = csv_file_paths[1:2]

# --- load transactions ---
print('\nloading transactions')
transactions = []
num_files = len(csv_file_paths)
current_file = 1
for file_path in csv_file_paths:
    with open(file_path, 'r') as input_file:
        input_file.readline()
        for line in input_file:
            transaction = read_transaction(line)

            if transaction:
                transactions.append(transaction)
            
        print(f'proccessed file {file_path} ({current_file}/{num_files})')
        current_file += 1

transactions = transactions[:10000]
# --- remove transactions with large numbers of inputs or outputs ---

print('\nremoving large transactions')
transactions = [transaction for transaction in transactions if not is_large(transaction)]

# --- untangle / classify transactions ---

print('\nclassifying transactions')
num_simple = 0
num_separable = 0
num_ambiguous = 0

timing_dict = {
    'simple': [],
    'separable': [],
    'ambiguous': []
}

for transaction in transactions:
    classif_start = perf_counter()
    
    transaction_hash = transaction[0]
    inputs = transaction[1]
    outputs = transaction[2]

    if len(inputs) == 1 or len(outputs) == 1:
        num_partitions = 0
        type = 'simple'
        num_simple += 1
    else:
        print(f'classifying transaction {transaction_hash}',end='\r')

        partitions = untangle(transaction)
        
        num_partitions = len(partitions)
        if partitions:
            if num_partitions == 1:
                type = 'separable'
                num_separable += 1
            else:
                type = 'ambiguous'
                num_ambiguous += 1
        else:
            type = 'simple'
            num_simple += 1

    classif_end = perf_counter()
    timing_dict[type].append(classif_end - classif_start)
    num_word = 'partition' if num_partitions == 1 else 'partitions'
    cardinality = f'{len(inputs):>3}:{len(outputs):<3}'
    print(f'found {num_partitions:4} {num_word:10} for transaction {transaction_hash} - cardinality: {cardinality} classification: {type:9} time: {classif_end - classif_start:.2e}s')

total_transactions = len(transactions)
percent_simple = (num_simple / total_transactions) * 100
percent_separable = (num_separable / total_transactions) * 100
percent_ambiguous = (num_ambiguous / total_transactions) * 100

program_end = perf_counter()
execution_time = (program_end - program_start)

print()
print(f'  total transactions: {total_transactions:^,}')
print(f'              simple: {num_simple:^6,} ({percent_simple:05.2f}%)')
print(f'           separable: {num_separable:^6,} ({percent_separable:05.2f}%)')
print(f'           ambiguous: {num_ambiguous:^6,} ({percent_ambiguous:05.2f}%)')

simple_time = sum(timing_dict['simple'])
simple_avg = simple_time / num_simple
simple_worst = max(timing_dict['simple'])

separable_time = sum(timing_dict['separable'])
separable_avg = separable_time / num_separable
separable_worst = max(timing_dict['separable'])

ambiguous_time = sum(timing_dict['ambiguous'])
ambiguous_avg = ambiguous_time / num_ambiguous
ambiguous_worst = max(timing_dict['ambiguous'])

print()
print(f'total execution time: {execution_time/60:05.2f} minutes')
print(f'classification times:')
print(f'              simple: {simple_time:04.2f}s ({simple_avg:04.2f}s average) ({simple_worst:04.2f} worst)')
print(f'           separable: {separable_time:04.2f}s ({separable_avg:04.2f}s average) ({separable_worst:04.2f} worst)')
print(f'           ambiguous: {ambiguous_time:04.2f}s ({ambiguous_avg:04.2f}s average) ({ambiguous_worst:04.2f} worst)')