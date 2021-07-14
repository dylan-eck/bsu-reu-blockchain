from itertools import permutations

class Transaction:
	def __init__(self, hash='', inputs=[], outputs=[], fee=0, type='unclassified'):
		self.hash = hash
		self.inputs = inputs
		self.outputs = outputs
		self.fee = fee
		self.type = type
		
	def is_coinbase(self):
		return self.inputs == [('coinbase',)]

	def from_csv_string(self, csv_string):
		fields = csv_string.split(',')

		self.hash = fields[0]

		input_addresses = fields[1].split(':')
		
		if fields[2]:
			input_values = [int(x) for x in fields[2].split(':')]
		else:
			input_values = []

		self.inputs = list(zip(input_addresses, input_values))

		output_addresses = fields[3].split(':')
		output_values = [int(x) for x in fields[4].split(':')]
		self.outputs = list(zip(output_addresses, output_values))

		self.fee = int(fields[5])

	def to_csv_string(self):
		input_addresses = ':'.join([input[0] for input in self.inputs])
		input_values = ':'.join([str(input[1]) for input in self.inputs])

		output_addresses = ':'.join([output[0] for output in self.outputs])
		output_values = ':'.join([str(output[1]) for output in self.outputs])

		csv_string = f'{self.hash},{input_addresses},{input_values},{output_addresses},{output_values},{self.fee},{self.type}\n'
		return csv_string

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