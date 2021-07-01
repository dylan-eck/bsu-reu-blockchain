from urllib.request import urlopen
from time import perf_counter, strftime
from datetime import datetime
from dateutil import tz
from dateutil.relativedelta import *
import logging
import json
import os

def load_json(filepath):
    with open(filepath, 'r') as fp:
        data = json.load(fp)
    return data

def save_json(filepath, data):
    with open(filepath, 'w') as output_file:
        json.dump(data, output_file)

def get_days(start_day,end_day):
    """
    inputs:     two datetime objects

    returns:    datetime objects for every day from start_day up to, but not including end_day
    """
    days = []

    current_day = start_day
    while current_day < end_day:
        days.append(current_day)
        current_day += relativedelta(days=+1)

    return days

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

if __name__ == '__main__':

    # set start and end days
    time_period_days = 1
    end_day = datetime(year=2021, month=6, day=28, tzinfo = tz.gettz('Etc/GMT'))
    start_day = end_day - relativedelta(days=time_period_days)
    
    days = get_days(start_day, end_day)

    # create a directory to store the block json files
    if not os.path.exists('block_data'):
        os.mkdir('block_data')

    for day in days:
        day_string = day.strftime("%Y-%m-%d")

        # create a sub-directory for each day to help keep things organized
        if not os.path.exists(f'block_data/{day_string}'):
            os.mkdir(f'block_data/{day_string}')

        print(f'collecting blocks from {day_string}\n')
        
        block_summaries = get_block_summaries(day)
        num_blocks = len(block_summaries)

        print(f'collected block summary file containing {num_blocks} blocks')

        file_name = f'block_data/{day_string}/blocks_{day_string}.json'
        save_json(file_name,block_summaries)

        n = 1
        for block in block_summaries:
            t1 = perf_counter()
            block_hash = block.get('hash')

            block_data = get_block(block_hash)

            filename = f'block_data/{day_string}/{block_hash}.json'
            save_json(filename, block_data)

            t2 = perf_counter()
            block_time = t2-t1 
            print(f'collected block {block_hash} ({n}/{num_blocks}) in {block_time:.2f}s')
            
            n += 1


        print()
