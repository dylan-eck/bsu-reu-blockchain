from functions import get_file_names, load_transactions_from_csv
from itertools import chain

csv_file_directory = '../csv_files/'
input_directory = f'{csv_file_directory}raw_transactions_unclassified/'
output_directory = f'{csv_file_directory}raw_transactions_unclassified/'

csv_file_names = get_file_names(input_directory, "[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$")

address_dict = dict()

for file in csv_file_names:
    print(f'loading file {file}')
    transactions = load_transactions_from_csv(f'{input_directory}{file}')

    for transaction in transactions:
        print(f'processing transaction {transaction.hash}')
        num_inputs = len(transaction.inputs)
        num_outputs = len(transaction.outputs)

        is_mtm = num_inputs > 1 and num_outputs > 1
        input_addresses = [input[0] for input in transaction.inputs]
        output_addresses = [output[0] for output in transaction.outputs]

        for address in (input_addresses + output_addresses):
            if address in address_dict:
                if not address_dict[address] and is_mtm:
                    address_dict[address] = True
            else:
                address_dict[address] = is_mtm

out_file_name = 'address_list.csv'
with open(f'{output_directory}{out_file_name}','w') as output_file:
    output_file.write('address,is_many_to_many\n')
    for address in address_dict:
        output_file.write(f'{address},{address_dict[address]}')