import collections
from typing import List, Tuple
import argparse
import sys
import os

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input',
                    dest='input_directory',
                    type=str,
                    help='a directory containing theinput files'
                    )

parser.add_argument('-o', '-output',
                    dest='output_directory',
                    type=str,
                    help='the directoy where output files are written'
                    )

args = parser.parse_args()
print(args.input_directory)