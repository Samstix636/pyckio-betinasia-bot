from pprint import pprint
import json
import requests
from dotenv import load_dotenv
import os

load_dotenv()

class SpMarketClient():
    
    def __init__(self):
        self.base_url = 'https://api.mollybet.com'
        
        self.USERNAME = os.getenv('USER')
        self.PASSWORD = os.getenv('PASSWORD')

    def login(self):
        headers = {
            "Content-Type": 'application/json; charset=utf-8',
            "Accept": "application/json"
        }
        data = {'username': self.USERNAME, 'password': str(self.PASSWORD)}
        print(data)
        s = requests.Session()
        r = s.post(url=self.base_url + '/v1/sessions/', data=json.dumps(data), headers=headers).json()
        print(r)
        token_file = open('token.txt', "w", encoding='utf-8')
        token_file.write(r['data'])
        token_file.close()
        
        return s, r['data']
        # return s, "0ca9c57f76fb314b0a42c38016be67b0"

    def get_balance(self, s, token):
        headers = {
            "Content-Type": 'application/json; charset=utf-8',
            "Accept": "application/json",
            'Session': token
        }
        r = s.get(url= self.base_url + f'/v1/customers/{self.USERNAME}/accounting_info/', headers=headers).json()
        data = r['data']
        
        if r['status'] == 'ok':
            val = [d['value'] for d in data if d['key'] == 'available_credit'][0]
        else:
            val = None
        return val

    def get_order_details(self, s, token, order_id):
        headers = {
            "Content-Type": 'application/json; charset=utf-8',
            "Accept": "application/json",
            'Session': token
        }

        r = s.get(url=self.base_url + f'/v1/orders/{order_id}/', headers=headers).json()
        data = r['data']
        # pprint(r)
        if data['status'] == 'reconciled':
            status, fill_price, fill_stake, pnl = data['status'], data['price'],  data['stake'][-1], data['profit_loss'][-1]
        else:
            status, fill_price, fill_stake, pnl = data['status'], '-', '-', '-'

        return status, fill_price, fill_stake, pnl
    
    def get_customer_info(self, s, token):
        headers = {
            "Content-Type": 'application/json; charset=utf-8',
            "Accept": "application/json",
            'Session': token
        }
        data = {"inactive": True}

        r = s.get(url=self.base_url + f'/v1/customers/{self.USERNAME}/bookie_accounts/', headers=headers).json()
        # data = r['data']
        # accs = [d['bookie'] for d in data]
        pprint(r, sort_dicts = False)

    def open_betslip(self, s, token, bet_data:dict):
        
        headers = {
            "Content-Type": 'application/json; charset=utf-8',
            "Accept": "application/json",
            'Session': token
        }
        data = {'sport': bet_data['sport'], 'bet_type': bet_data['bet_type'], 'event_id': bet_data['event_id']}#'want_bookies': [bookie]
        r = s.post(url=self.base_url + '/v1/betslips/', data=json.dumps(data), headers=headers).json()
        data = r['data']
        # print('---------------------- Opening Betslip ---------------------')
        # print('betslip response: ', data)
        if r['status'] != 'error':
            betslid_id = data['betslip_id']
            if data['is_open'] == True:
                return betslid_id
            else:
                return None
        else:
            return None

    def place_bet(self, s, token, betslip_id: str, stake_: float, price_: float):
        price = float(price_)
        headers = {
            "Content-Type": 'application/json; charset=utf-8',
            "Accept": "application/json",
            'Session': token
        }
        
        data = {'betslip_id': betslip_id, 'price': price, 'stake': ['EUR', stake_], 'duration': 259200, 'adaptive_bookies': []}#, 'accounts': accounts, 'duration': 300.0
        
        r = s.post(url=self.base_url + '/v1/orders/', json=data, headers=headers).json()
        # print(r)
        
        if r['status'] != 'error':
            print('================== BET PLACED ====================')
            print(r['data']['bet_type_description'], '@', r['data']['want_price'], ' for ', r['data']['event_info']['event_name'], '(',r['data']['event_info']['competition_name'],')')
            data = r['data']
            order_id = data['order_id']
            placement_time = data['placement_time']
            bet_description = data['bet_type_description']
        else:
            order_id = None
            placement_time = None
            bet_description = None
        return order_id, placement_time, bet_description
        # return data

