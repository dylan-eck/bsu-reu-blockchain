from urllib.request import urlopen
from dateutil.relativedelta import *
import json
import os
import re
import csv

def get_days(start_day,end_day):
    """
    inputs:     two datetime objects

    returns:    datetime objects for every day from start_day up to, but not including end_day
    """
    days = []

    current_day = start_day
    while current_day <= end_day:
        days.append(current_day)
        current_day += relativedelta(days=+1)

    return days

# blockchain.com data api calls

def get_block_summaries(day):
    """
    inputs:     a datetime object

    returns:    basic information about each block added to the blockchain on the given day
                including the block hash and block height
    """
    timestamp = int(day.timestamp() * 1000)

    url = f'https://blockchain.info/blocks/{str(timestamp)}?format=json'
    response = urlopen(url)

    block_summaries = json.load(response)
    return block_summaries

def get_block(block_hash):
    """
    inputs:     a block hash

    returns:    a dictionary containg all information related to the specifed block
                including header information and a list of transactions contained within the block
    """

    url = f'https://blockchain.info/rawblock/{block_hash}'
    response = urlopen(url)

    block_data = json.load(response)
    return block_data

# block data processing

def get_inputs(transaction):
    inputs = []

    for input in transaction.get('inputs'):
        prev_out = input.get('prev_out')
        
        if prev_out is not None:
            addr = prev_out.get('addr')
            value = prev_out.get('value')
        else:
            addr = 'coinbase'
            value = ''

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

            input_addr_str = ''
            input_val_str = ''
            for input in inputs:
                if not None in input:
                    input_addr_str += f'{input[0]}:'
                    input_val_str += f'{input[1]}:'
            input_addr_str = input_addr_str[:-1]
            input_val_str = input_val_str[:-1]

            output_addr_str = ''
            output_val_str = ''
            for output in outputs:
                if not None in output:
                    output_addr_str += f'{output[0]}:'
                    output_val_str += f'{output[1]}:'
            output_addr_str = output_addr_str[:-1]
            output_val_str = output_val_str[:-1]

            transaction_data.append([transaction_hash, input_addr_str, input_val_str, output_addr_str, output_val_str, transaction_fee])

    return transaction_data

def get_block_file_paths(directory):
    block_file_paths = []

    for (root, dirs, files) in os.walk(directory):
        for file in files:
            pattern = re.compile("^[a-z0-9]{64}.json$")
            if pattern.match(file):
                file_path = os.path.join(root, file)
                block_file_paths.append(file_path)

    return block_file_paths

# file I/O functions

def load_json(filepath):
    with open(filepath, 'r') as fp:
        data = json.load(fp)
    return data

def save_json(filepath, data):
    with open(filepath, 'w') as output_file:
        json.dump(data, output_file)

def write_csv(file_name, data):
    with open(file_name, 'w') as output_file:
        writer = csv.writer(output_file)
        writer.writerows(data)