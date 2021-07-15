import os
import re
import csv

from transaction import Transaction

def get_immediate_subdirectories(directory_path):
    immediate_subdirectories = []
    for x in os.scandir(directory_path):
        if x.is_dir():
            immediate_subdirectories.append(x)

    return immediate_subdirectories

def get_file_names(direcotry, pattern):
    file_names = []
    for (root, dirs, files) in os.walk(direcotry):
        for file in files:
            pattern = re.compile(pattern)
            if pattern.match(file):
                file_names.append(file)
    return file_names

def load_transactions_from_csv(csv_file_path):
    transactions = []

    with open(csv_file_path, 'r') as input_file:
        input_file.readline()
        for line in input_file:
            transaction = Transaction()
            transaction.from_csv_string(line)

            if not transaction.is_coinbase():
                transactions.append(transaction)

    return transactions

def write_csv(file_name, data):
    with open(file_name, 'w') as output_file:
        writer = csv.writer(output_file)
        writer.writerows(data)