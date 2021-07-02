from time import perf_counter
from datetime import datetime
from dateutil import tz
from dateutil.relativedelta import *
import os

from functions import get_days, get_block_summaries, get_block, save_json, load_json

# configure logging
import logging
logging.basicConfig(filename='logs/app.log',filemode='w',format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)

program_start = perf_counter()

# set start and end days
time_period_days = 75
end_day = datetime(year=2021, month=6, day=28, tzinfo = tz.gettz('Etc/GMT'))
start_day = end_day - relativedelta(days=time_period_days)

days = get_days(start_day, end_day)

# create a directory to store the block json files
try:
    if not os.path.exists('block_data'):
        os.mkdir('block_data')
except:
    message = 'could not create block_data directory'
    logging.critical(message)
    raise

failed_days = set()
failed_blocks = set()

for day in days:
    day_start = perf_counter()

    day_string = day.strftime("%Y-%m-%d")

    try:
        # create a sub-directory for each day to help keep things organized
        if not os.path.exists(f'block_data/{day_string}'):
            os.mkdir(f'block_data/{day_string}')
    except:
        failed_days.add(day_string)

        message = f'could not create directory for day {day_string}'
        logging.critical(message)
        raise

    logging.info(f'collecting blocks from {day_string}\n')
    
    try:
        block_summaries = get_block_summaries(day)

    except:
        failed_days.add(day_string)

        logging.error(f'failed to load block summaries for {day_string}')

    else:
        num_blocks = len(block_summaries)

        logging.info(f'collected block summary file containing {num_blocks} blocks')

        file_name = f'block_data/{day_string}/blocks_{day_string}.json'
        save_json(file_name,block_summaries)

        n = 1
        for block in block_summaries:
            block_start = perf_counter()
            
            block_hash = block.get('hash')

            try:
                block_data = get_block(block_hash)

            except:

                if day_string in failed_blocks:
                    failed_blocks[day_string].add(block_hash)
                
                else:
                    failed_blocks[day_string] = {block_hash}

                logging.error(f'failed to load block {block_hash}')

            else:
                filename = f'block_data/{day_string}/{block_hash}.json'
                save_json(filename, block_data)

                block_end = perf_counter()
                block_time = block_end-block_start
                logging.info(f'collected block {block_hash} ({n}/{num_blocks}) - block processing time: {block_time:.2f}s')
                
                n += 1

        day_end = perf_counter()
        day_tiem = (day_end-day_start)/60
        logging.info(f'collected {num_blocks} blocks from {day_string} - day processing time: {block_time:.2f} minutes\n')

# report errors that occured

num_failed_days = len(failed_days)
failed_days_message = f'blocks for the following {num_failed_days} days could not be retrieved:\n\n'

for day in failed_days:
    failed_days_message += f'    {day}\n'
failed_days_message += '\n'

logging.error(failed_days_message)

num_failed_blocks = len(failed_blocks)
failed_blocks_message = f'the following {num_failed_blocks} individual blocks could not be retrieved:\n\n'

for day in failed_blocks:
    failed_blocks_message += f'    {day}\n'

    for hash in failed_blocks.get(day):
        failed_blocks_message += f'        {hash}\n'
failed_blocks_message += '\n'

logging.error(failed_blocks_message)

program_end = perf_counter()
execution_time = (program_end-program_start)/60/60
logging.info(f'execution finished in {execution_time:.2f} hours\n')



