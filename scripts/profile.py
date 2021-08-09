from transaction import Transaction
from functions import load_transactions_from_csv, profile_transactions

transactions = []

file_name = '../scalability/4M_txs/raw_transactions_classified/transactions_1.csv'
transactions = load_transactions_from_csv(file_name)

num_transactions = len(transactions)

tx_dict = profile_transactions(transactions)
for (key, value) in tx_dict.items():
    percent = (value / num_transactions) * 100
    print(f'{key:>14}: {value:>12,} ({percent:>6.2f}%)')