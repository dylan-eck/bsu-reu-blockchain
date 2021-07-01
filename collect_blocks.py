from time import perf_counter, strftime
from datetime import datetime
from dateutil import tz
from dateutil.relativedelta import *
import json
import os

from functions import get_days, get_block_summaries, get_block, save_json, load_json

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
