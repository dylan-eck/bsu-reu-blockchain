from urllib.request import urlopen
from time import perf_counter, strftime
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

def get_block_summaries(day):
    timestamp = int(day.timestamp() * 1000)

    url = f'https://blockchain.info/blocks/{str(timestamp)}?format=json'
    response = urlopen(url)

    blocks = json.load(response)
    return blocks

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

def proccess_block(block_summary):
    block_hash = block_summary.get('hash')

    url = f'https://blockchain.info/rawblock/{block_hash}'
    response = urlopen(url)

    block = json.load(response)

    transactions = block.get('tx')

    transaction_list = []

    for transaction in transactions:
        transaction_hash = transaction.get('hash')
        inputs = get_inputs(transaction)
        outputs = get_outputs(transaction)

        if inputs and outputs:
            transaction_list.append([transaction_hash, inputs, outputs])

    return transaction_list

if __name__ == '__main__':

    t_i = perf_counter()

    time_period_days = 1

    end_day = datetime(year=2021, month=6, day=28, tzinfo = tz.gettz('Etc/GMT'))
    start_day = end_day - relativedelta(days=time_period_days)
    
    days = get_days(start_day, end_day)

    for day in days:
        day_start = perf_counter()

        print(f'collecting transactions from {day.strftime("%Y-%m-%d")}\n')

        blocks = get_block_summaries(day)

        num_blocks = len(blocks)
        print(f'collected {num_blocks} blocks')

        n = 1
        transaction_list = []
        for block in blocks:
            print(f'processing block {block.get("height")} ({n}/{num_blocks})')
            transaction_list += proccess_block(block)

            n += 1

        print(f'\nwriting transactions to file\n')

        with open(f'transactions.csv', 'a', buffering=1000) as output_file:
            output_file.write('transaction_hash,input_addreses,input_values,output_addresses,output_values\n')

            for transaction in transaction_list:
                hash = transaction[0]
                inputs = transaction[1]
                outputs = transaction[2]

                input_addr_str = inputs[0][0]
                input_val_str = str(inputs[0][1])
                for i in range(1,len(inputs)):
                    input_addr_str += f':{inputs[i][0]}'
                    input_val_str += f':{inputs[i][1]}'


                output_addr_str = outputs[0][0]
                output_val_str = str(outputs[0][1])
                for i in range(1,len(outputs)):
                    output_addr_str += f':{outputs[i][0]}'
                    output_val_str += f':{outputs[i][1]}'


                output_file.write(f'{hash},{input_addr_str},{input_val_str},{output_addr_str},{output_val_str}\n')

        day_end = perf_counter()
        day_time = (day_end-day_start)/60
        print(f'day proccessed in {day_time:.2f} minutes')

    t_f = perf_counter()
    execution_time_m = (t_f-t_i) / 60
    print(f'total execution time: {execution_time_m:.2f} minutes')

