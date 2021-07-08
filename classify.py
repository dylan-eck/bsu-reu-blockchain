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
    input_addresses = line[1].split(':')
    input_values = [int(x) for x in line[2].split(':')]
    output_addresses = line[3].split(':')
    output_values = [int(x) for x in line[4].split(':')]
    transaction_fee = int(line[5])

    inputs = list(zip(input_addresses, input_values))
    outputs = list(zip(output_addresses, output_values))

    return [transaction_hash, inputs, outputs, transaction_fee]

def sort_key(input):
    return input[1]

def remove_small_inputs(inputs, transaction_fee):
    inputs.sort(key=sort_key,reverse=True)
    for (address, value) in inputs:
        if value < transaction_fee:
            inputs.remove((address, value))
            transaction_fee -= value
    return (inputs, transaction_fee)

def is_large(transaction):
    if len(transaction[1]) > 5 or len(transaction[2]) > 5:
        return True
    else:
        return False

def get_codewords(n):
	k = n
	codewords = []
	codeword = [1 for _ in range(0, n)]
	while True:
		codewords.append(codeword.copy())
		startIndex = n - 1
		while startIndex >= 0:
			if not codeword[0 : startIndex]:
				return codewords
			else:
				maxValue = max(codeword[0 : startIndex])
				codewordAtStartIndex = codeword[startIndex]
				if maxValue > k or codewordAtStartIndex > maxValue or codewordAtStartIndex >= k:
					codeword[startIndex] = 1
					startIndex -= 1
				else:
					codeword[startIndex] += 1
					break

def get_partitions(list):
	n = len(list)
	codewords = get_codewords(n)

	partitions = []
	for codeword in codewords:
		partition = []
		num_subsets = max(codeword)
		for i in range(num_subsets):
			partition.append([])

		for i in range(len(codeword)):
			element = list[i]
			subset = codeword[i]
			partition[subset-1].append(element)
		
		partitions.append(partition)
	return partitions

t1 = perf_counter()

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
            transactions.append(transaction)
            
        print(f'proccessed file {file_path} ({current_file}/{num_files})')
        current_file += 1

transactions = transactions[:10000]
# -- remove transactions with large numbers of inputs or outputs

print('\nremoving large transactions')
transactions = [transaction for transaction in transactions if not is_large(transaction)]

# --- remove small inputs ---
ir_start = perf_counter()
print('\nremoving small inputs')
num_affected_txs = 0
total_inputs_removed = 0
for transaction in transactions:
    old_num_inputs = len(transaction[1])
    old_num_outputs = len(transaction[2])
    old_fee = transaction[3]

    res = remove_small_inputs(transaction[1],transaction[3])
    transaction[1] = res[0]
    transaction[3] = res[1]
    new_fee = transaction[3]

    new_num_inputs = len(transaction[1])
    new_num_outputs = len(transaction[2])
    
    change = old_num_inputs - new_num_inputs
    if change > 0:
        num_affected_txs += 1
        total_inputs_removed += change

        old_cardinality = f'{old_num_inputs:>3}:{old_num_outputs:<3}'
        new_cardinality = f'{new_num_inputs:>3}:{new_num_outputs:<3}'

        temp = 'input' if change == 1 else 'inputs'
        print(f'removed {change:3} small {temp} from transaction {transaction[0]}')
        print(f'    old cardinality: {old_cardinality} new cardinality: {new_cardinality}')
        print(f'            old fee: {old_fee:^7,}         new fee: {new_fee:^7,}\n')
ir_end = perf_counter()
print(f'removed {total_inputs_removed:,} small inputs from {num_affected_txs:,} transactions time: {(ir_end-ir_start)/60:.2f} minutes')

def acceptable(input_partition,output_partition,fee):
    acceptable = True
    for in_subset in input_partition:
        for out_subset in output_partition:
            if not connectable(in_subset,out_subset,fee):
                acceptable = False
    return acceptable

def group_by_size(partitions):
    size_dict = {}
    for partition in partitions:
        if not len(partition) in size_dict:
            size_dict[len(partition)] = [partition]
        else:
            size_dict[len(partition)].append(partition)
    return size_dict

# --- remove small outputs ---
print('\nremoving small outputs')
current_transaction = 1
for transaction in transactions:
    transaction_hash = transaction[0]
    inputs = transaction[1]
    outputs = transaction[2]

    # generate all input and output partitions
    input_partitions = get_partitions(inputs)
    output_partitions = get_partitions(outputs)

    # group partitions by size
    input_partitions = group_by_size(input_partitions)
    output_partitions = group_by_size(output_partitions)

    print(f'generated input and output partitions for transaction {transaction_hash} ({current_transaction}/{len(transactions)})')
    current_transaction += 1

    # generate all possible orderings of all possible partitions

    # acceptable_partitions = []
    # for input_partition in input_partitions:
    #     for output_partition in output_partitions:
    #         pass

    # print(f'{transaction_hash} input partitions: {input_partitions}')



    


# print('\nclassifying transactions')

# total_mtm= len(mtm_transactions)
# num_simple = total_transactions - total_mtm
# num_separable = 0
# num_ambiguous = 0
# current_transaction = 1
# for transaction in mtm_transactions:
#     t_start = perf_counter()

#     transaction_hash = transaction[0]
#     input_addrs = transaction[1]
#     input_vals = transaction[2]
#     output_addrs = transaction[3]
#     output_vals = transaction[4]
#     fee = transaction[5]

#     input_sets = []
#     for i in range(1,len(input_vals)):
#         input_sets += list(combinations(input_vals,i))

#     output_sets = []
#     for i in range(1,len(output_vals)):
#         output_sets += list(combinations(output_vals,i))

#     num_connectable_sets = 0

#     in_adj_list = dict()
#     out_adj_list = dict()
#     for output_set in output_sets:
#         for input_set in input_sets:
#             if connectable(input_set,output_set,fee):
#                 num_connectable_sets += 1
#                 if output_set in out_adj_list:
#                     out_adj_list[output_set].append(input_set)
#                 else:
#                     out_adj_list[output_set] = [input_set]

#                 if input_set in in_adj_list:
#                     in_adj_list[input_set].append(output_set)
#                 else:
#                     in_adj_list[input_set] = [output_set]

#     if num_connectable_sets > 2:
#         ambiguous = False
#         for input_set in in_adj_list:
#             output_sets = in_adj_list[input_set]
#             if len(input_sets) > 1:
#                 ambiguous = True

#         for output_set in out_adj_list:
#             input_sets = out_adj_list[output_set]
#             if len(input_sets) > 1:
#                 ambiguous = True

#         if ambiguous:
#             type = 'ambiguous'
#             num_ambiguous += 1
#         else:
#             type = 'separable'
#             num_separable += 1

#     else:
#         type = 'simple'
#         num_simple += 1

#     num_inputs = len(input_addrs)
#     num_outputs = len(output_addrs)
#     t_finish = perf_counter()
#     print(f'classified transaction {transaction_hash} cardinality: {num_inputs:3}:{num_outputs:<3} type: {type:10} #: {current_transaction:8,}/{total_mtm:<8,} time: {t_finish-t_start:4.2e}s')
#     current_transaction += 1

# print()

# percent_simple = (num_simple / total_transactions) * 100
# percent_separable = (num_separable / total_transactions) * 100
# percent_ambiguous = (num_ambiguous / total_transactions) * 100

# t2 = perf_counter()

# print(f'total transactions: {total_transactions:,}')
# print(f'            simple: {num_simple:,} ({percent_simple:.2f}%)')
# print(f'         separable: {num_separable:,} ({percent_separable:.2f}%)')
# print(f'         ambiguous: {num_ambiguous:,} ({percent_ambiguous:.2f}%)')
# print(f'\n    execution time: {(t2-t1)/60:.2f} minutes')