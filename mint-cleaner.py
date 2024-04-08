import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob


transactions = pd.read_csv(f"/data/transactions.csv", parse_dates=['Date'], encoding='utf8').sort_values('Date').reset_index()
transactions['Id'] = transactions.index

# Map the Account Name column to the right Firefly account id
account_map = {
    "Checking Account": 1,
    "Savings Account": 2
}

transactions['AccountId'] = transactions['Account Name'].map(account_map)

transactions[transactions['AccountId'].isnull()].groupby('Account Name').count()
print("Missing account ids: %s: %s" % (len(transactions[~transactions['AccountId'].notnull()]), transactions[~transactions['AccountId'].notnull()]['Account Name'].unique()))
transactions = transactions[transactions['AccountId'].notnull()]
transactions['AccountId'] = transactions['AccountId'].astype('int')

transactions['AbsoluteAmount'] = transactions[['Amount', 'Transaction Type']].apply(lambda x: x['Amount'] if x['Transaction Type'] == 'credit' else -x['Amount'], axis=1)
transactions['CumSum'] = transactions.sort_values('Date').groupby('Account Name')['AbsoluteAmount'].transform(pd.Series.cumsum)

dfs = []
for name in glob.glob('/data/mint-balances/*.csv'):
    dfs.append(pd.read_csv(name, parse_dates=['Date']))
actual_balances = pd.concat(dfs, axis=0, ignore_index=True)
current_balance = actual_balances.sort_values(by='Date', ascending=False).groupby('Account Name').first().drop(columns=['Date'])

def render_bal_chart(acc_name, acc_name2):
    fig, (ax, ax2) = plt.subplots(2, figsize=(20, 20))
    ax.title.set_text(f'Difference in expected vs actual: {acc_name}')
    ax.xaxis.set_label('Date')

    applicable_transactions = transactions[transactions['Account Name'] == acc_name2]
    
    actual_calc = actual_balances[actual_balances['Account Name'] == acc_name].set_index('Date')
    actual_calc['Change'] = actual_calc['Amount'].diff()
    actual = actual_calc[actual_calc['Change'] != 0]
    estimated = applicable_transactions.groupby(['Date'])['AbsoluteAmount'].sum().cumsum()

    merge = pd.merge(left=actual['Amount'].rename('Actual'), right=estimated.rename('Estimated'), left_index=True, right_index=True, how='inner', validate='one_to_one')
    merge['Error'] = merge['Actual'] - merge['Estimated']
    
    current_error = abs(merge['Error'].iloc[-1])
    legend_handles = []
    best_errors = []
    for index, duplicate in applicable_transactions[applicable_transactions.duplicated(subset=['Amount', 'Transaction Type', 'Date', 'Original Description'])].groupby(['Date', 'Transaction Type', 'Original Description']).agg(['count', 'sum', 'mean'])['Amount'].iterrows():   
        date_key = duplicate.name[0]
        
        if duplicate.name[1] == 'debit':
            adj = -duplicate['mean']
        else:
            adj = duplicate['mean']
        test_s = merge['Error'].copy()
        test_s.loc[test_s.index > date_key] += adj
        test_s2 = merge['Error'].copy()
        test_s2.loc[test_s.index > date_key] += (adj * duplicate['count'])
        new_error = abs(test_s[date_key:].mean())
        
        if new_error < current_error:
            best_errors.append({'error': test_s2.abs().sum(), 'data': duplicate, 'adj': adj}) # Area under the curve
    
    best_errors = sorted(best_errors, key=lambda x: x['error'])
    new_df = merge['Error'].copy()
    current_error = new_df.abs().sum()
    convergence = [current_error]
    for best_error in best_errors:
        attempt = new_df.copy()
        date_key = best_error['data'].name[0]
        adj = best_error['adj']
        attempt.loc[date_key:] += adj
        new_error = attempt.abs().sum()
        if new_error < current_error:
            new_df = attempt
            current_error = new_error
            convergence.append(current_error)
            print(f"Found duplicate that when removed, reduced error: {date_key.strftime('%Y-%m-%d')}, tamount={adj},iamount={adj/duplicate['count']}")
            foo = ax.plot(new_df, label=f"Duplicate: {date_key.strftime('%Y-%m-%d')}, amount={best_error['data']['mean']}")[0]
            legend_handles.append(foo)
            ax.axvline(x=date_key, color=foo.get_color())
    
    ax2.title.set_text('Convergence of error')
    ax2.plot(convergence)

    ax.legend(handles=legend_handles)
    ax.axhline(y=0, color='grey', dashes=[2])

render_bal_chart('Checking Account')
