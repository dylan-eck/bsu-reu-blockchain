from itertools import permutations
import multiprocessing as mp

from transaction import Transaction
from functions import get_file_names, load_transactions_from_csv

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
    partitions = get_acceptable_partitions(transaction)
    sub_transactions = transactions_from_partitions(transaction, partitions)
    return sub_transactions

if __name__ == '__main__':
    num_processes = mp.cpu_count()
    pool = mp.Pool(processes=num_processes)
    print(f'found {num_processes} available threads\n')

    csv_file_directory = '../csv_files/'
    input_directory = f'{csv_file_directory}simplified_transactions/'
    output_directory = f'{csv_file_directory}untangled_transactions/'

    csv_file_names = get_file_names(input_directory, "[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$")

    for file in csv_file_names:
        print(f'processing file {input_directory}{file}:')
        transactions = load_transactions_from_csv(f'{input_directory}{file}')

        separable_txs = [tx for tx in transactions if tx.type == 'separable']

        untangled_txs = pool.map(func, separable_txs)

        # untangled_txs = [item for sublist in untangled_txs for item in sublist]

        flat_txs = []
        if untangled_txs:
            for index, tx_list in enumerate(untangled_txs):
                from classify import classify
                untangled_txs[index] = [classify(tx) for tx in tx_list]

        for item in untangled_txs:
            str = [x.type for x in item]
            if 'separable' in str:
                print(f'    {str}')
        
    
    # print(f'    writing new csv file... {output_directory}{file}', end='\r', flush=True)
    # with open(f'{output_directory}{file}', 'w') as output_file:
    #     output_file.write('transaction_hash,num_inputs,input_addresses,input_values,num_outputs,output_addresses,output_values,transaction_fee,transaction_class\n')
    #     for transaction in transactions:
    #         output_file.write(transaction.to_csv_string())
    # print(f'{"    writing new csv file... done":<100}')
