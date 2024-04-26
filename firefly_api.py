import pandas as pd
import json
import requests

fireflyToken = ""
fireflyDomain = "https://firefly.example.com"
session = requests.Session()

def send_to_firefly(record: pd.Series):
    item = {
        'type': record['type'],
        'amount': record['amount']
    }
    for key in ['description',  'destination_name', 'source_name', 'category_name', 'notes']:
        if key in record and not pd.isnull(record[key]):
            item[key] = record[key]
    for key in ['source_id', 'destination_id']:
        if key in record and not pd.isnull(record[key]) and record[key] != '':
            item[key] = str(int(record[key]))
    item['foreign_amount'] = '0'
    item['reconciled'] = False
    item['order'] = '0'
    item['currency_id'] = '8'
    item['currency_code'] = 'USD'
    for key in ['date', 'process_date']:
        if key in record and not pd.isnull(record[key]) and record[key] != '':
            item[key] = record[key].isoformat()
    if isinstance(record['tags'], str):
        item['tags'] = [record['tags']]
    else:
        item['tags'] = []
    payload = {
            "transactions": [
                item
            ],
            "apply_rules": True,
            "fire_webhooks": False,
            "error_if_duplicate_hash": True
        }
    try:
        json.dumps(payload)
    except Exception as e:
        raise Exception(f"Can't serialize '{item}: {e}")
    resp = session.post(f"{fireflyDomain}/api/v1/transactions", headers={"Authorization": f"Bearer {fireflyToken}", "Content-Type": "application/json", 'Accept': 'application/json'}, json=payload, allow_redirects=False)
    if resp.status_code == 200:
        return pd.Series([resp.status_code, resp.json(), resp.json()['data']['id']], index=['status', 'message', 'firefly_id'])
    else:
        return pd.Series([resp.status_code, resp.json()], index=['status', 'message'])