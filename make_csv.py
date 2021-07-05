import json
import os
import re

def get_inputs(transaction):
    inputs = []

    for input in transaction.get('inputs'):
        prev_out = input.get('prev_out')
        
        if prev_out is not None:
            addr = prev_out.get('addr')
            value = prev_out.get('value')
            inputs.append([addr, value]) 
    
    return inputs

def get_outputs(transaction):
    outputs = []

    for output in transaction.get('out'):
        addr = output.get('addr')
        value = output.get('value')

        if addr is not None:
            outputs.append([addr,value])

    return outputs

def get_tx_data(block):
    transactions = block.get('tx')

    transaction_data = []

    for transaction in transactions:
        transaction_hash = transaction.get('hash')
        transaction_fee = transaction.get('fee')
        inputs = get_inputs(transaction)
        outputs = get_outputs(transaction)

        if inputs and outputs:
            transaction_data.append([transaction_hash, inputs, outputs, transaction_fee])

    return transaction_data

block_data_directory = 'block_data/'
block_file_paths = []

for (root, dirs, files) in os.walk(block_data_directory):
    for file in files:
        pattern = re.compile("^[a-z0-9]{64}.json$")
        if pattern.match(file):
            file_path = os.path.join(root, file)
            block_file_paths.append(file_path)

num_transactions = 0

for file_path in block_file_paths:
    with open(file_path) as input_file:
        block = json.load(input_file)
        block_hash = block.get('hash')
        transaction_data = get_tx_data(block)
        num_transactions += len(transaction_data)
        input_file.close()
        
        print(f'loaded block {block_hash}')

print(num_transactions)

# if not os.path.exists('csv_files'):
#     os.mkdir('csv_files')

# with open('csv_files/transactions.csv', 'a', buffering=1000) as output_file:
#     output_file.write('transaction_hash,input_addresses,input_values,output_addresses,output_values,transaction_fee\n')

#     for transaction in transaction_data:
#         hash = transaction[0]
#         inputs = transaction[1]
#         outputs = transaction[2]
#         fee = transaction[3]

#         input_addr_str = inputs[0][0]
#         input_val_str = str(inputs[0][1])
#         for i in range(1,len(inputs)):
#             input_addr_str += f':{inputs[i][0]}'
#             input_val_str += f':{inputs[i][1]}'


#         output_addr_str = outputs[0][0]
#         output_val_str = str(outputs[0][1])
#         for i in range(1,len(outputs)):
#             output_addr_str += f':{outputs[i][0]}'
#             output_val_str += f':{outputs[i][1]}'


#         output_file.write(f'{hash},{input_addr_str},{input_val_str},{output_addr_str},{output_val_str},{fee}\n')









    
