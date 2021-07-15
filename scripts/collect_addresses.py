from functions import get_file_names, load_transactions_from_csv

csv_file_directory = '../csv_files/'
input_directory = f'{csv_file_directory}raw_transactions_unclassified/'
output_directory = f'{csv_file_directory}raw_transactions_unclassified/'

print('locating input_files... ',end='',flush=True)
csv_file_names = get_file_names(input_directory, "[0-9]{4}-[0-9]{2}-[0-9]{2}.csv$")
print('done')

address_dict = dict()

for file in csv_file_names:
    print(f'loading file {file}... ',end='',flush=True)
    transactions = load_transactions_from_csv(f'{input_directory}{file}')
    print('done')

    for transaction in transactions:
        print(f'processing transactions... {transaction.hash}',end='\r',flush=True)
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
    print(f'{f"processing transactions... done":<100}')

out_file_name = 'address_list.csv'
print(f'writing output file {out_file_name}')
with open(f'{output_directory}{out_file_name}','w') as output_file:
    output_file.write('address,in_many_to_many\n')
    for address in address_dict:
        print(f'writing addresses... {address}',end='\r',flush=True)
        output_file.write(f'{address},{address_dict[address]}\n')
    print(f'{f"writing addresses... done":<100}')