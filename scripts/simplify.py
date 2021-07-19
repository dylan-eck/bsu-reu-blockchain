from time import perf_counter
import multiprocessing as mp
import os

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

if __name__ == '__main__':
	num_processes = mp.cpu_count()
	pool = mp.Pool(processes=num_processes)
	print(f'found {num_processes} available threads\n')

	csv_file_directory = '../csv_files/'
	input_directory = f'{csv_file_directory}raw_transactions_classified/'
	output_directory = f'{csv_file_directory}simplified_transactions/'

	if not os.path.exists(output_directory):
		os.mkdir(output_directory)

	csv_file_names = get_file_names(input_directory, "[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$")

	for file_name in csv_file_names:
		simp_start = perf_counter()

		print(f'processing file {file_name}:')
		print('    loading transactions... ', end='\r', flush=True)
		transactions = load_transactions_from_csv(f'{input_directory}{file_name}')
		print(f'{"    loading transactions... done":85}')

		print(f'    simplifying transactions... ', end='', flush=True)
		simplified_transactions = pool.map(simplify, transactions)
		print(f'done')

		print(f'    writing new csv file {output_directory}{file_name}... ', end='', flush=True)
		with open(f'{output_directory}{file_name}', 'w') as output_file:
			output_file.write('transaction_hash,num_inputs,input_addresses,input_values,num_outputs,output_addresses,output_values,transaction_fee,transaction_class\n')
			for transaction in simplified_transactions:
				output_file.write(transaction.to_csv_string())
		print('done')

		simp_end = perf_counter()
		print(f'    finished in {simp_end - simp_start:.2f}s\n')
