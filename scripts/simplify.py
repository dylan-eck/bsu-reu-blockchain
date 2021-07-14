from transaction import Transaction
from functions import get_file_names, load_transactions_from_csv
from untangle import get_acceptable_partitions

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

csv_file_directory = '../csv_files/'
input_directory = f'{csv_file_directory}raw_transactions_classified/'
output_directory = f'{csv_file_directory}simplified_transactions/'

csv_file_names = get_file_names(input_directory, "[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$")

for file_name in csv_file_names:
    print(f'processing file {file_name}:')
    print('    loading transactions... ', end='\r', flush=True)
    transactions = load_transactions_from_csv(f'{input_directory}{file_name}')
    print(f'{"    loading transactions... done":85}')

    for index, transaction in enumerate(transactions):
        hash = transaction.hash
        print(f'    simplifying transactions... {hash}', end='\r', flush=True)

        transaction = consolodate_same_addresses(transaction)
        transaction = remove_small_inputs(transaction)
        transaction = remove_small_outputs(transaction)
        transactions[index] = transaction
    print(f'{"    simplifying transactions... done":85}')
    
