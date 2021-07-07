from time import perf_counter
from itertools import combinations
import os
import re

def connectable(input_vals, output_vals, fee):
    input_sum = sum(input_vals)
    output_sum = sum(output_vals)
    a = output_sum + fee
    if a >= input_sum and input_sum >= output_sum:
        return True
    
    else:
        return False

def read_transaction(line):
    line = line.split(',')
    transaction_hash = line[0]
    input_addrs = line[1].split(':')
    input_vals = [int(x) for x in line[2].split(':')]
    output_addrs = line[3].split(':')
    output_vals = [int(x) for x in line[4].split(':')]
    transaction_fee = int(line[5])

    return (transaction_hash, input_addrs, input_vals, output_addrs, output_vals, transaction_fee)

t1 = perf_counter()

print('locating csv files\n')
csv_file_paths = []
for (root, dirs, files) in os.walk('csv_files'):
        for file in files:
            pattern = re.compile("transactions_[0-9]*.csv$")
            if pattern.match(file):
                file_path = os.path.join(root, file)
                csv_file_paths.append(file_path)
csv_file_paths = csv_file_paths[1:2]

print('loading transactions')
in_out_upper_limit = 11
mtm_transactions = []
total_transactions = 0
num_files = len(csv_file_paths)
current_file = 1
for file_path in csv_file_paths:
    with open(file_path, 'r') as input_file:
        input_file.readline()
        for line in input_file:
            transaction = read_transaction(line)
            transaction_hash = transaction[0]
            num_inputs = len(transaction[1])
            num_outputs = len(transaction[3])

            if num_inputs > 1 and num_inputs < in_out_upper_limit and num_outputs > 1 and num_outputs < in_out_upper_limit:
                mtm_transactions.append(transaction)
            
            total_transactions += 1
        print(f'proccessed file {file_path} ({current_file}/{num_files})')
        current_file += 1

print('\nclassifying transactions')

total_mtm= len(mtm_transactions)
num_simple = total_transactions - total_mtm
num_separable = 0
num_ambiguous = 0
current_transaction = 1
for transaction in mtm_transactions:
    t_start = perf_counter()

    transaction_hash = transaction[0]
    input_addrs = transaction[1]
    input_vals = transaction[2]
    output_addrs = transaction[3]
    output_vals = transaction[4]
    fee = transaction[5]

    input_sets = []
    for i in range(1,len(input_vals)):
        input_sets += list(combinations(input_vals,i))

    output_sets = []
    for i in range(1,len(output_vals)):
        output_sets += list(combinations(output_vals,i))

    num_connectable_sets = 0

    in_adj_list = dict()
    out_adj_list = dict()
    for output_set in output_sets:
        for input_set in input_sets:
            if connectable(input_set,output_set,fee):
                num_connectable_sets += 1
                if output_set in out_adj_list:
                    out_adj_list[output_set].append(input_set)
                else:
                    out_adj_list[output_set] = [input_set]

                if input_set in in_adj_list:
                    in_adj_list[input_set].append(output_set)
                else:
                    in_adj_list[input_set] = [output_set]

    if num_connectable_sets > 2:
        ambiguous = False
        for input_set in in_adj_list:
            output_sets = in_adj_list[input_set]
            if len(input_sets) > 1:
                ambiguous = True

        for output_set in out_adj_list:
            input_sets = out_adj_list[output_set]
            if len(input_sets) > 1:
                ambiguous = True

        if ambiguous:
            type = 'ambiguous'
            num_ambiguous += 1
        else:
            type = 'separable'
            num_separable += 1

    else:
        type = 'simple'
        num_simple += 1

    num_inputs = len(input_addrs)
    num_outputs = len(output_addrs)
    t_finish = perf_counter()
    print(f'classified transaction {transaction_hash} cardinality: {num_inputs:3}:{num_outputs:<3} type: {type:10} #: {current_transaction:8,}/{total_mtm:<8,} time: {t_finish-t_start:4.2e}s')
    current_transaction += 1

print()

percent_simple = (num_simple / total_transactions) * 100
percent_separable = (num_separable / total_transactions) * 100
percent_ambiguous = (num_ambiguous / total_transactions) * 100

t2 = perf_counter()

print(f'total transactions: {total_transactions:,}')
print(f'            simple: {num_simple:,} ({percent_simple:.2f}%)')
print(f'         separable: {num_separable:,} ({percent_separable:.2f}%)')
print(f'         ambiguous: {num_ambiguous:,} ({percent_ambiguous:.2f}%)')
print(f'\n    execution time: {(t2-t1)/60:.2f} minutes')