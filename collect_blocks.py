from urllib.request import urlopen
from time import perf_counter, strftime
from datetime import datetime
from dateutil import tz
from dateutil.relativedelta import *
import logging
import json

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

def get_block(block_summary):
    """
    inputs:     a block summary dictionary (get_block_summaries() returns a list of these)

    returns:    a dictionary containg all information related to the specifed block
                including header information and a list of transactions contained within the block
    """
    block_hash = block_summary.get('hash')

    url = f'https://blockchain.info/rawblock/{block_hash}'
    response = urlopen(url)

    block_data = json.load(response)
    return block_data

if __name__ == '__main__':

    logging.basicConfig(filename='app.log', filemode='w', format='[%(asctime)s] : %(message)s')

    time_period_days = 90

    end_day = datetime(year=2021, month=6, day=28, tzinfo = tz.gettz('Etc/GMT'))
    start_day = end_day - relativedelta(days=time_period_days)
    
    days = get_days(start_day, end_day)

    success_count = 0
    fail_count = 0
    failed_dates = []

    for day in days:
        day_string = day.strftime("%Y-%m-%d")

        # print/log current date value
        date_msg = "DATE: " + str(day_string)
        print("\n" + date_msg)
        logging.error(date_msg)

        try:
            block_summaries = get_block_summaries(day)

            for block in block_summaries:
                block_data = get_block(block)

                block_hash = block_data.get('hash')

                # file format: <block hash>_<block day/time>.json
                filename = str(block_hash) + "_" + str(day_string) + ".json"

                # print/log current HASH value for A date
                hash_msg = "HASH SAVED: " + str(block_hash)
                print(hash_msg)
                logging.error(hash_msg)

                save_json(filename, block_data)

            success_count = success_count + 1
        except:
            fail_count = fail_count + 1
            failed_dates.append(day_string)

            # print/log error
            failed_msg = "ERROR - failed to load transactions for " + str(day_string)
            print(failed_msg)
            logging.error(failed_msg)
    
    print("\nRESULTS:")
    print(" - success count:", success_count)
    print(" - failure count:", fail_count)
    print(" - failed dates: ")
    for fdate in failed_dates:
        print("    " + str(fdate))



            


