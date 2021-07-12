from untangle import Transaction, untangle
from time import perf_counter
import os
import re

def read_transaction(line):
    line = line.split(',')
    hash = line[0]

    input_addresses = line[1].split(':')
    if input_addresses == ['coinbase']:
        return None
    input_values = [int(x) for x in line[2].split(':')]
    inputs = list(zip(input_addresses, input_values))

    output_addresses = line[3].split(':')
    output_values = [int(x) for x in line[4].split(':')]
    outputs = list(zip(output_addresses, output_values))

    fee = int(line[5])

    transaction = Transaction(hash, inputs, outputs, fee)
    return transaction

# def write_transaction(file, transaction):
#     hash = transaction[0]
#     input_addresses = [input[0] for input in transaction[1]]
#     input_values = [input[1] for input in transaction[1]]
#     output_addresses = [output[0] for output in transaction[2]]
#     output_values = [output[1] for output in transaction[2]]
#     fee = transaction[3]

#     file.write(f'{hash},{input_addresses},{input_values},{output_addresses},{output_values},{fee}\n')

def is_large(transaction):
    size_limit = 8
    if len(transaction.inputs) >= size_limit or len(transaction.outputs) >= size_limit:
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
    
    if len(transaction.inputs) == 1 or len(transaction.outputs) == 1:
        num_partitions = 0
        transaction.type = 'simple'
        num_simple += 1

    elif is_large(transaction):
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

    # print information about the most recent transaction that was proccessed
    classif_end = perf_counter()
    timing_dict[transaction.type].append(classif_end - classif_start)
    num_word = 'partition' if num_partitions == 1 else 'partitions'
    cardinality = f'{len(transaction.inputs):>3}:{len(transaction.outputs):<3}'
    print(f'found {num_partitions:4} {num_word:10} for transaction {transaction.hash} - cardinality: {cardinality} classification: {transaction.type:11} time: {classif_end - classif_start:.2e}s')

# --- print information summarizing all of the transactions that were processed ---
total_transactions = len(transactions)
percent_simple = (num_simple / total_transactions) * 100
percent_separable = (num_separable / total_transactions) * 100
percent_ambiguous = (num_ambiguous / total_transactions) * 100
percent_intract = (num_intract / total_transactions) * 100

simple_time = sum(timing_dict['simple'])
simple_avg = simple_time / num_simple
simple_worst = max(timing_dict['simple'])

separable_time = sum(timing_dict['separable'])
separable_avg = separable_time / num_separable
separable_worst = max(timing_dict['separable'])

ambiguous_time = sum(timing_dict['ambiguous'])
ambiguous_avg = ambiguous_time / num_ambiguous
ambiguous_worst = max(timing_dict['ambiguous'])

intract_time = sum(timing_dict['intractable'])
intract_avg = intract_time / num_intract
intract_worst = max(timing_dict['intractable'])

program_end = perf_counter()
execution_time = (program_end - program_start)

print()
print(f'  total transactions: {total_transactions:^7,}')
print(f'              simple: {num_simple:^7,} ({percent_simple:5.2f}%)')
print(f'           separable: {num_separable:^7,} ({percent_separable:5.2f}%)')
print(f'           ambiguous: {num_ambiguous:^7,} ({percent_ambiguous:5.2f}%)')
print(f'         intractable: {num_intract:^7,} ({percent_intract:5.2f}%)')

print()
print(f'total execution time: {execution_time/60:07.2f} minutes')
print(f'              simple: {simple_time:7.2f}s ({simple_avg:5.2f}s average) ({simple_worst:5.2f}s worst)')
print(f'           separable: {separable_time:7.2f}s ({separable_avg:5.2f}s average) ({separable_worst:5.2f}s worst)')
print(f'           ambiguous: {ambiguous_time:7.2f}s ({ambiguous_avg:5.2f}s average) ({ambiguous_worst:5.2f}s worst)')
print(f'         intractable: {intract_time:7.2f}s ({intract_avg:5.2f}s average) ({intract_worst:5.2f}s worst)')