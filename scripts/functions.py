
from dateutil.relativedelta import *
import json
import os
import re
import csv

def write_csv(file_name, data):
    with open(file_name, 'w') as output_file:
        writer = csv.writer(output_file)
        writer.writerows(data)