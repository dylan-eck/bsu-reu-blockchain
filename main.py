from urllib.request import urlopen
from time import perf_counter
from datetime import datetime
from dateutil import tz
from dateutil.relativedelta import *
import json

def get_days(start_day,end_day):
    days = []

    current_day = start_day
    while current_day < end_day:
        days.append(current_day)
        current_day += relativedelta(days=+1)

    return days

def get_blocks(day):
    timestamp = int(day.timestamp() * 1000)

    url = 'https://blockchain.info/blocks/' + str(timestamp) + '?format=json'
    response = urlopen(url)

    blocks= json.load(response)
    return blocks

def get_transactions(block):
    url = 'https://blockchain.info/rawblock/' + block.get('hash')
    response = urlopen(url)

    block_data = json.load(response)

    transactions = block_data.get('tx')
    return transactions

def get_input_addresses(transaction):
    inputs = []

    for input in transaction.get('inputs'):
        prev_out = input.get('prev_out')
        
        if prev_out is not None:
            addr = prev_out.get('addr')
            inputs.append(addr)  
    
    return inputs

def get_output_addresses(transaction):
    outputs = []

    for output in transaction.get('out'):
        addr = output.get('addr')

        if addr is not None:
            outputs.append(addr)

    return outputs

if __name__ == '__main__':

    t1 = perf_counter()

    time_period_days = 1

    end_day = datetime(year=2021, month=6, day=28, tzinfo = tz.gettz('Etc/GMT'))
    start_day = end_day - relativedelta(days=time_period_days)
    
    days = get_days(start_day, end_day)

    print(f'collecting blocks:')
    blocks = []
    for day in days:
        day_blocks = get_blocks(day)
        blocks += day_blocks
        print(f'collected {len(day_blocks)} blocks from {day.strftime("%Y-%m-%d")} ({int(day.timestamp()) * 1000})')

    print(f'\ncollected {len(blocks)} total blocks')

    blocks = blocks[0:1]

    print('\ncollecting transactions:')
    transactions = []
    for block in blocks:
        block_transactions = get_transactions(block)
        transactions += block_transactions

        block_height = block.get('height')
        print(f'collected {len(block_transactions)} transactions from block {block_height}')

    address_adj_list = {}

    input_dict = {}
    output_dict = {}
    for transaction in transactions:
        hash = transaction.get('hash')

        inputs = get_input_addresses(transaction)
        outputs = get_output_addresses(transaction)

        input_dict[hash] = inputs
        output_dict[hash] = outputs

        # for input in inputs:
        #     if input in address_adj_list:
        #         address_adj_list[input] += outputs
        #     else:
        #         address_adj_list[input] = outputs

    t2 = perf_counter()

    execution_time_m = (t2-t1) / 60

    print(f'\ncollected {len(transactions)} total transactions\n')
    print(f'execution time: {execution_time_m:.2f} minutes')

    # for i in range(5):
    #     hash = transactions[i].get('hash')

    #     inputs = input_dict.get(hash)
    #     outputs = output_dict.get(hash)

    #     print(f'transaction: {hash}\n')

    #     print('inputs:')
    #     for input in inputs:
    #         print(f'    {input}')
    #     print()

    #     print('outputs:')
    #     for output in outputs:
    #         print(f'    {output}')
    #     print()