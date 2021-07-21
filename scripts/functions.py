from itertools import permutations
import os
import re
import csv

from transaction import Transaction

# --- file operations ---

def get_immediate_subdirectories(directory_path):
    immediate_subdirectories = []
    for x in os.scandir(directory_path):
        if x.is_dir():
            immediate_subdirectories.append(x)

    return immediate_subdirectories

def get_file_names(direcotry, pattern):
    file_names = []
    for (root, dirs, files) in os.walk(direcotry):
        for file in files:
            pattern = re.compile(pattern)
            if pattern.match(file):
                file_names.append(file)
    return file_names

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

def write_csv(file_name, data):
    with open(file_name, 'w') as output_file:
        writer = csv.writer(output_file)
        writer.writerows(data)

# --- transaction untangling ---

def single_partition_to_string(partition):
    '''
    input:        a partition of a set

    output:     the partition formated as a string that is easily readable
    '''
    partition_string = ''
    for i in range(len(partition)):
        subset_string = f'[{partition[i][0]}'
        for j in range(1,len(partition[i])):
            subset_string += f' {partition[i][j]}'
        subset_string += ']'
        partition_string += f'{subset_string} - '
    partition_string = partition_string[:-3]
    return partition_string

def tx_partition_to_string(tx_partition):
    '''
    input:        a transaction partition

    returns:    the transaction partion formated as a string that is easily readable
    '''
    tx_partition_string = ''

    input_partition = tx_partition[0]
    output_partition = tx_partition[1]
    # input and output partitions should be the same size
    partition_size = len(input_partition) 

    for i in range(partition_size):
        input = [item[0] for item in input_partition[i]]
        output = [item[0] for item in output_partition[i]]
        tx_partition_string += f'{input} --> {output}\n'

    return tx_partition_string

# code for the following function modified from https://github.com/mrqc/partitions-of-set
def get_codewords(n, k):
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

def get_partitions(list,max_size):
    n = len(list)
    codewords = get_codewords(n,max_size)

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

def group_partitions_by_size(partitions):
    '''
    input:  a list of partitions (lists)

    returns:  a dictionary where each key is an integer, and the corresponding value 
    is a list of partitions with the corresponding number of subsets
    '''
    partition_dict = {}
    for partition in partitions:
        size = len(partition)
        if size in partition_dict:
            partition_dict[size].append(partition)
        else:
            partition_dict[size] = [partition]
    return partition_dict

def is_connectable(input_subset,output_subset,transaction_fee):
    input_sum = sum([item[1] for item in input_subset])
    output_sum = sum([item[1] for item in output_subset])

    a = output_sum + transaction_fee
    return(a >= input_sum and input_sum >= output_sum)

def get_acceptable_connections(partition_size,input_partition,output_partition,transaction_fee):
    acceptable_connections = []

    # check the input partition against all possible orderings of the output partition
    output_orders = list(permutations(output_partition,len(output_partition)))
    for output_ordering in output_orders:
        acceptable = True
        for i in range(partition_size):
            pair_connectable = is_connectable(input_partition[i],output_ordering[i],transaction_fee)
            if not pair_connectable:
                acceptable = False
                break
        if acceptable:
            partition = (input_partition,output_ordering)
            acceptable_connections.append(partition)

    return acceptable_connections

def get_acceptable_partitions(transaction):
    num_inputs = len(transaction.inputs)
    num_outputs = len(transaction.outputs)

    max_partition_size = min(num_inputs,num_outputs)

    input_partitions = get_partitions(transaction.inputs,max_partition_size)
    output_partitions = get_partitions(transaction.outputs,max_partition_size)

    input_partitions = group_partitions_by_size(input_partitions)
    output_partitions = group_partitions_by_size(output_partitions)

    acceptable_partitions = []
    for i in range(2,max_partition_size+1):
        for input_partition in input_partitions[i]:
            for output_partition in output_partitions[i]:
                acceptable_partitions += get_acceptable_connections(i,input_partition,output_partition,transaction.fee)
    
    return acceptable_partitions

def untangle(transaction):
    acceptable_partitions = get_acceptable_partitions(transaction)
    return acceptable_partitions

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

def func(transaction):
    # print(f'    untangling transactions... [{os.getpid()}] {transaction.hash}', end='\r', flush=True)

    # print(f'[{os.getpid()}] untangling transaction {transaction.hash}')
    if transaction.type == 'separable':
        partitions = get_acceptable_partitions(transaction)
        sub_transactions = transactions_from_partitions(transaction, partitions)
        return sub_transactions

    else:
        return [transaction]

# --- transaction simplification ---

def consolodate_same_addresses(transaction):
	# consolodate input addresses
	input_dict = {}
	for input in transaction.inputs:
		if input[0] in input_dict:
			input_dict[input[0]] += input[1]
		else:
			input_dict[input[0]] = input[1]
	transaction.inputs = list(input_dict.items())

	# consolodate output addresses
	output_dict = {}
	for output in transaction.outputs:
		if output[0] in output_dict:
			output_dict[output[0]] += output[1]
		else:
			output_dict[output[0]] = output[1]
	transaction.outputs = list(output_dict.items())

	return transaction

def sort_key(input):
	return input[1]

def remove_small_inputs(transaction):

	transaction.inputs.sort(key=sort_key)
	inputs_to_remove = []
	for input in transaction.inputs:
		if input[1] <= transaction.fee:
			inputs_to_remove.append(input)
			transaction.fee -= input[1]
		else:
			break

	transaction.inputs = [x for x in transaction.inputs if not x in inputs_to_remove]
	return transaction

def remove_small_outputs(transaction):
	acceptable_partitions = get_acceptable_partitions(transaction)

	# find the largets minimum change in value from inputs to outputs over all subsets from all partitions
	delta = 0
	for partition in acceptable_partitions:
		input_partition = partition[0]
		output_partition = partition[1]
		partition_size = len(input_partition)

		input_subset = input_partition[0]
		output_subset = output_partition[0]

		# find the smallest net change in value from inputs to outputs over all subsets in the partition
		min_flow = sum([x[1] for x in input_subset]) - sum([x[1] for x in output_subset])
		for i in range(1,partition_size):
			input_subset = input_partition[i]
			output_subset = output_partition[i]

			flow = sum([x[1] for x in input_subset]) - sum([x[1] for x in output_subset])
			if flow < min_flow:
				min_flow = flow
		
		if min_flow > delta:
			delta = min_flow

	transaction.outputs.sort(key=sort_key)
	outputs_to_remove = []
	for output in transaction.outputs:
		if output[1] <= delta:
			outputs_to_remove.append(output)
			transaction.fee += output[1]

	transaction.outputs = [x for x in transaction.outputs if not x in outputs_to_remove]
	return transaction

def simplify(transaction):
	if transaction.type != 'intractable':
		old_num_inputs = len(transaction.inputs)
		old_num_outputs = len(transaction.outputs)

		transaction = consolodate_same_addresses(transaction)
		transaction = remove_small_inputs(transaction)
		transaction = remove_small_outputs(transaction)

		new_num_inputs = len(transaction.inputs)
		new_num_outputs = len(transaction.outputs)

		if new_num_inputs != old_num_inputs or new_num_outputs != old_num_outputs:
			transaction.type = 'unclassified'

	return transaction

# --- transaction classification ---

def classify(transaction):
    try:
        if transaction.type == 'unclassified':

                tx_size_limit = 8
                if transaction.inputs == [('coinbase','')]:
                    transaction.type = 'simple'

                elif len(transaction.inputs) == 1 or len(transaction.outputs) == 1:
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
    except:
        print(f'        failed to simplify transaction {transaction.hash}')

    return transaction

# --- misc ---

def profile(transactions):
    type_dict = {
        'total': 0,
        'unclassified': 0,
        'simple': 0,
        'separable': 0,
        'ambiguous': 0,
        'intractable': 0
    }

    for transaction in transactions:
        type_dict['total'] += 1
        type_dict[transaction.type] += 1

    return type_dict
