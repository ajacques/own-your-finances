import pandas as pd
from datetime import timedelta
from typing import Union, List
from functools import reduce

transactions = pd.read_csv(f"transactions.csv", parse_dates=['Date'], encoding='utf8').sort_values('Date')

# Map the Account Name column to the right Firefly account id
account_map = {
    "Checking Account": 1,
    "Savings Account": 2
    # ...
}

transactions['AccountId'] = transactions['Account Name'].map(account_map)

transactions[transactions['AccountId'].isnull()].groupby('Account Name').count()
print("Missing account ids: %s: %s" % (len(transactions[~transactions['AccountId'].notnull()]), transactions[~transactions['AccountId'].notnull()]['Account Name'].unique()))
transactions = transactions[transactions['AccountId'].notnull()]
transactions['AccountId'] = transactions['AccountId'].astype('int')

def account(num: Union[int, List[int]]):
    if type(num) == int:
        return transactions['AccountId'] == num
    else:
        return transactions['AccountId'].isin(num)
    
def orig_descr_contains(msg: str):
    return transactions['Original Description'].str.contains(msg, case=False)

# List of all Firefly account ids that are credit cards
credit_cards = [5, 10, 15, 20]

account_id_401k = 25

known_account_ids = {
    'X1234': 1,
    # If you have multiple accounts at one bank
    '$FOO BANK TRANSFER': [1, 2, 3]
}

not_a_transfer = [
	# Maybe exclude an entire account. For example, your 401k may never have
	# a transfer (unless you take a loan or do a Mega Backdoor Roth)
    account(account_id_401k),

	# Or you want to exclude reimbursements via Venmo
    account(23) & orig_descr_contains('John Doe Paid '), # Venmo
    account(23) & ~transactions['Category'].isna() & (transactions['Category'] != 'Transfer'), # Venmos frequently appear like transfers because a friend reimburses me

	# Credit Card Payments (to the card) can be transfers, but
	# maybe they're never transfers going out (unless you do a Balance Transfer)
    account(credit_cards) & (transactions['Transaction Type'] == 'debit'),

	# A paycheck
	# While it is a transfer from your employer to you, without the
	# debit side, the algorithm could incorrectly classify it as a transfer
    orig_descr_contains('PAYROLL'),
]

df_filter_relevant = ~reduce(lambda x, y: x | y, not_a_transfer)

# Build a cache of transactions matching certain descriptions. Speeds up transfer identification below
known_account_ids_cached = {}
descr_cached = {}

for key, value in known_account_ids.items():
    series = transactions['Original Description'].str.contains(key)
    
    if isinstance(value, list):
        descr_cached[key] = transactions['AccountId'].isin(value)
        for id in value:
            if id in known_account_ids_cached:
                known_account_ids_cached[id] = known_account_ids_cached[id] | series
            else:
                known_account_ids_cached[id] = series
    else:
        descr_cached[key] = transactions['AccountId'] == value
        if value in known_account_ids_cached:
            known_account_ids_cached[value] = known_account_ids_cached[value] | series
        else:
            known_account_ids_cached[value] = series

def attempt(df, base, attempt):
    if len(df[base & attempt]) > 0:
        return base & attempt
    else:
        return base

def find_transfer(row, df, time_window_days=5):
    start_date = row['Date']
    end_date = start_date + timedelta(days=time_window_days)

    opposing_filter = (
        (df['Account Name'] != row['Account Name']) &
        (df['Date'] >= start_date) &
        (df['Date'] <= end_date) &
        (df['Amount'] == row['Amount']) &
        (df['Transaction Type'] != row['Transaction Type']) &
        (~df['Considered']) &
        df_filter_relevant
    )

    match_by_description = set()
    inverted_by_account_id_in_description = []
    not_inverted = []
    for key, value in known_account_ids.items():
        # If we find an account id in this record's description
        # then refine the other side by the found account id(s)
        if key in row['Original Description'].upper():
            if isinstance(value, list):
                for id in value:
                    match_by_description.add(id)
            else:
                match_by_description.add(value)
        
        # Find possible descriptions based on this row's
        # account id
        if (isinstance(value, list) and row['AccountId'] in value) or (isinstance(value, int) and value == row['AccountId']):
            pass
        else:
            not_inverted.append(~descr_cached[key])
        
    if len(match_by_description) > 0:
        opposing_filter = attempt(df, opposing_filter, df['AccountId'].isin(match_by_description))

    if row['AccountId'] in known_account_ids_cached:
        filter_by_invert = known_account_ids_cached[row['AccountId']]
        if len(not_inverted) > 0:
            filter_by_invert = filter_by_invert & reduce(lambda x, y: x & y, not_inverted)
        opposing_filter, success = attempt(df, opposing_filter, filter_by_invert)
    
    transfer_pair = df[opposing_filter]    
    if not transfer_pair.empty:
        df.at[row.name, 'Considered'] = True
        opposite = transfer_pair.iloc[0]
        return opposite
    else:
        return None

def filter_for_non_empty(inputD):
    return set(filter(lambda x: isinstance(x, str) and x != '', inputD))

def process_record(row, df):
    if df.at[row.name, 'Considered']:
        return None
    
    if df_filter_relevant.loc[row.name]:
        pair = find_transfer(row, df)
        if pair is not None:
            df.at[pair.name, 'Considered'] = True
            notes = [
                row['Notes'],
                pair['Notes'],
            ]
            labels = filter_for_non_empty([
                row['Labels'],
                pair['Labels']
            ])
            category = ''.join(filter_for_non_empty([
                row['Category'],
                pair['Category']
            ]))
            if row['Transaction Type'] == 'debit':
                # Bank is on the left
                date = row['Date']
                process_date = pair['Date']
                notes.append(pair['Original Description'])
                return {
                    'type': 'transfer',
                    'date': row['Date'],
                    'process_date': pair['Date'],
                    'amount': row['Amount'],
                    'category_name': category,
                    'description': row['Original Description'],
                    'source_id': row['AccountId'],
                    'destination_id': pair['AccountId'],
                    'tags': labels,
                    'format': 'transfer_debit',
                    'notes': '\n'.join(filter(lambda x: isinstance(x, str) and x != '', notes))
                }
            else:
                date = pair['Date']
                process_date = row['Date']
                notes.append(row['Original Description'])
                return {
                    'type': 'transfer',
                    'date': date,
                    'process_date': process_date,
                    'amount': row['Amount'],
                    'category_name': category,
                    'description': pair['Original Description'],
                    'source_id': pair['AccountId'],
                    'destination_id': row['AccountId'],
                    'tags': labels,
                    'format': 'transfer_credit',
                    'notes': '\n'.join(filter(lambda x: isinstance(x, str) and x != '', notes))
                }
        
        result = {
            'date': row['Date'],
            'description': row['Original Description'],
            'amount': row['Amount'],
            'category': row['Category'],
            'tags': row['Labels'],
            'notes': row['Notes']
        }
        
        if row['Transaction Type'] == 'credit':
            result['type'] = 'deposit'
            result['destination_id'] = row['AccountId']
            result['source_name'] = row['Description']
        else:
            result['source_id'] = row['AccountId']
            result['destination_name'] = row['Description']
            result['type'] = 'withdrawal'
                
        return result

transactions['Considered'] = False

cleaned = transactions.apply(func=process_record, axis=1, result_type='expand', df=transactions)
cleaned = cleaned[~cleaned['amount'].isna()]

print(cleaned)