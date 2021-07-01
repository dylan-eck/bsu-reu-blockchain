from urllib.request import urlopen
import json

from datetime import datetime
from dateutil import tz
from dateutil.relativedelta import *

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

# json functions

def load_json(filepath):
    with open(filepath, 'r') as fp:
        data = json.load(fp)
    return data

def save_json(filepath, data):
    with open(filepath, 'w') as output_file:
        json.dump(data, output_file)