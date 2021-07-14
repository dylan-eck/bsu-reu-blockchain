from itertools import permutations
from transaction import Transaction

def single_partition_to_string(partition):
	'''
	input:		a partition of a set

	output: 	the partition formated as a string that is easily readable
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
	input:		a transaction partition

	returns:	the transaction partion formated as a string that is easily readable
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

if __name__ == '__main__':
	example_transactions = { 
		# example from figure 4 (ambiguous)
		'figure4': ['t0',[('a1',101),('a2',200),('a3',102),('a4',300)],[('b1',51),('b2',250),('b3',52),('b4',350)],10],

		# example from figure 5 (ambiguous)
		'figure5': ['t0',[('a1',11),('a2',27),('a3',5)],[('b1',5),('b2',6),('b3',32)],0],

		# example from figure 6 (separable)
		'figure6': ['t0',[('a1',20),('a2',10)],[('b1',19),('b2',7),('b3',3)],1],

		# example from figure 7 (ambiguous)
		'figure7': ['t0',[('a1',10),('a2',10)],[('b1',10),('b2',7),('b3',3)],0],

		# example from figure 10 (becomes separable after removing small inputs)
		'figure10': ['t0',[('a1',50),('a2',40),('a3',1)],[('b1',49),('b2',39)],3],
	}

	transaction = Transaction('t0', [('a1',101),('a2',200),('a3',102),('a4',300)], [('b1',51),('b2',250),('b3',52),('b4',350)], 10)

	acceptable_partitions = untangle(transaction)
	
	num_partitions = len(acceptable_partitions)
	num_word = 'partition' if num_partitions == 1 else 'partitions'
	print(f'\nfound {num_partitions} acceptable {num_word}:\n')
	for partition in acceptable_partitions:
		print(f'{tx_partition_to_string(partition)}')