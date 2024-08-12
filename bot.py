import requests
from time import sleep, time
import logging
import websocket
from Client import SpMarketClient
import ssl
from thefuzz import fuzz
from thefuzz import process
import pygsheets
import json
import sys
import threading
from threading import Thread
import unicodedata
logging.basicConfig(filename = f'logs.log', format='%(asctime)s - %(message)s', level=logging.INFO)

def main():
    
    url = f"https://api.pyckio.com/users/{pyckio_user_id}/mytimeline"

    payload = ""
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.6",
        "Authorization": "Bearer token", 
        "Connection": "keep-alive",
        "Origin": "https://pyckio.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Sec-GPC": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    # while True:
    ts = time() * 1000
    ts = int(ts)
    querystring = {"_":f"{ts}"}
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    # print(response.status_code)
    response_list = response.json()
    
    for latest_data in response_list[:10]:
        try:
            if latest_data['id'] not in parsed_ids:
                
                bet_data = {}
                bet_data['id'] = latest_data['id']
                bet_data['tipster'] = latest_data['userStatic']['username']
                bet_data['tipster_rank'] = latest_data['userStatic']['type']
                if bet_data['tipster_rank'] not in ['GRANDMASTER', 'PRO', 'PREPRO']:
                    continue
                event_slug = latest_data['eventSlug']
                sport, country, league = event_slug.split('-')[:3]
                bet_data['league'], bet_data['sport'], bet_data['country'] = league, sport, country
                event_name = latest_data['eventName']
                bet_data['home_team'], bet_data['away_team'] = event_name.split(' - ')
                bet_data['bet_id'] = latest_data['pick']['oddsType']
                bet_data['side'] = latest_data['pick']['bet']
                bet_data['line'] = latest_data['pick']['line']
                bet_data['pyckio_price'] = latest_data['pick']['price']
                bet_data['stake'] = latest_data['pick']['stake']
                
                mollybet_data = get_mollybet_bet_data(bet_data)
                if mollybet_data['event_id'] is not None:
                    # wss.register_event(mollybet_data['event_id'], mollybet_data['sport'])
                    
                    is_bet_placed = place_bet(mollybet_data)
                    if is_bet_placed:
                        parsed_ids.append(bet_data['id'])
                        update_sheets(bet_data['id'])
                        logging.info(f'pyckio_data: {bet_data}')
                        logging.info(f'bet placement data: {mollybet_data}')
                        logging.info('==============================================================')
                        sleep(1)
        except:
            logging.error('Error: ', exc_info=True)
            continue
    print('---------------------------------------------------------------------------------------------')

def update_sheets(bet_id):
    rows = wks1.get_all_values(include_tailing_empty=True, include_tailing_empty_rows=False, returnas='matrix')
    row_index = len(rows) + 1
    wks1.update_value(f'A{row_index}', bet_id)
    
     
def place_bet(bet_data):
    bet_slip_id = client.open_betslip(session, tn, bet_data)
    # print('Betslip ID: ', bet_slip_id)
    is_bet_placed = False
    if bet_slip_id is not None:
        order_id, placement_time, bet_description = client.place_bet(session, tn, bet_slip_id, stake_, bet_data['molly_odds'])
        if order_id is not None:
            is_bet_placed = True
            
    return is_bet_placed
        
        


def get_mollybet_bet_data(bet_data):
    pyckio_price = bet_data['pyckio_price']
    molly_price = 1+ ((pyckio_price - 1) - (0.02 * (pyckio_price-1)))
    molly_price = round(molly_price, 3)
    # line = bet_data['line']
    regular_sports = ['soccer', 'basketball', 'baseball']
    
    if bet_data['sport'] == 'tennis':
        bet_type = get_molly_tennis_bet_type(bet_data)
    elif bet_data['sport'] == 'baseball':
        bet_type = get_baseball_molly_bet_type(bet_data)
    elif bet_data['sport'] in regular_sports:
        bet_type = get_general_molly_bet_type(bet_data)
    else:
        bet_type = None
        
    event_id = get_molly_event_id(bet_data)
    
    molly_data = {}
    molly_data['event_id'] = event_id
    molly_data['bet_type'] = bet_type
    molly_data['molly_odds'] = molly_price
    molly_data['sport'] = get_molly_sport(bet_data['sport'])
    return molly_data
    
    
def get_molly_event_id(data):
    pyckio_competition_name = f"{data['country']}{data['league']}".lower()
    # print('pyckio_comp_name: ', pyckio_competition_name)
    pyckio_home_name, pyckio_away_name = data['home_team'], data['away_team']
    target_sport = get_molly_sport(data['sport'])
    sport_events = [e for e in event_stream if e['sport'] == target_sport]
    
    match_found = False
    for event in sport_events:
        molly_comp_name = event['competition_name'].replace(' ','').lower()
        comp_score = get_single_match_score(pyckio_competition_name, [molly_comp_name])
        home_name_score, away_name_score = get_double_match_score(normalise_name(pyckio_home_name), normalise_name(pyckio_away_name), [normalise_name(event['home'])], [normalise_name(event['away'])])
        if home_name_score >= 85 and away_name_score >= 85:
            match_found = True
            return event['event_id']
        elif comp_score >= 60:
            home_name_score, away_name_score = get_double_match_score(normalise_name(pyckio_home_name), normalise_name(pyckio_away_name), [normalise_name(event['home'])], [normalise_name(event['away'])])
            if (home_name_score >= 65 and away_name_score >= 65) or (home_name_score >= 50 and away_name_score >= 80) or (home_name_score >= 80 and away_name_score >= 50):
                match_found = True
                return event['event_id']
    
    if match_found is False and data['id'] not in not_matched:
        not_matched.append(data['id'])
        print(f'>>> No Match Found for: {pyckio_home_name} vs {pyckio_away_name}')
            
    return None

def get_molly_tennis_bet_type(data):
    line = data['line']
    if data['bet_id'] == '52':
        if data['side'] == 'HOME':
            bet_type = 'for,tset,all,vset1,p1'
        elif data['side'] == 'AWAY':
            bet_type = 'for,tset,all,vset1,p2'
    elif data['bet_id'] == '2':
        if data['side'] == 'OVER':
            bet_type = f'for,tset,all,vwhole,game,ahover,{int(line*4)}'
        elif data['side'] == 'UNDER':
            bet_type = f'for,tset,all,vwhole,game,ahunder,{int(line*4)}'
    elif data['bet_id'] == '3':
        if data['side'] == 'HOME':
            bet_type = f'for,tset,all,vwhole,game,ah,p1,{int(line*4)}'
        elif data['side'] == 'AWAY':
            bet_type = f'for,tset,all,vwhole,game,ah,p2,{int(line*-4)}'   
    elif data['bet_id'] == '4':
        if data['side'] == 'HOME':
            bet_type = f'for,tset,all,vwhole,set,ah,p1,{int(line*4)}'
        elif data['side'] == 'AWAY':
            bet_type = f'for,tset,all,vwhole,set,ah,p2,{int(line*-4)}'
    elif data['bet_id'] == '41':
        if data['side'] == 'HOME':
            bet_type = 'for,tset,1,vwhole,p1'
        elif data['side'] == 'AWAY':
            bet_type = 'for,tset,1,vwhole,p2'
    elif data['bet_id'] == '1':
        if data['side'] == 'HOME':
            bet_type = 'for,tset,all,vset1,p1'
        elif data['side'] == 'AWAY':
            bet_type = 'for,tset,all,vset1,p2'
    
    return bet_type

    
def get_molly_sport(pyckio_sport):
    if pyckio_sport == 'soccer':
        return 'fb'
    elif pyckio_sport == 'baseball':
        return 'baseball'
    elif pyckio_sport == 'basketball':
        return 'basket'
    elif pyckio_sport == 'tennis':
        return 'tennis'
    
def get_baseball_molly_bet_type(bet_data):
    line = bet_data['line']
    bet_type = None
    if bet_data['bet_id'] == '52':
        if bet_data['side'] == 'HOME':
            bet_type = 'for,tp,all,ml,h'
        elif bet_data['side'] == 'AWAY':
            bet_type = 'for,tp,all,ml,a'
            
    if bet_data['bet_id'] == '3':
        line = int(line *  4)
        if bet_data['side'] == 'HOME':
            bet_type = f'for,tp,all,ah,h,{line}'
        elif bet_data['side'] == 'AWAY':
            bet_type = f'for,tp,all,ah,a,{line * -1}'
    
    if bet_data['bet_id'] == '1':
        if bet_data['side'] == 'HOME':
            bet_type = f'for,tp,all,h'
        elif bet_data['side'] == 'AWAY':
            bet_type = 'for,tp,all,a'
        elif bet_data['side'] == 'DRAW':
            bet_type = 'for,tp,all,d'
            
    if bet_data['bet_id'] == '2':
        if bet_data['side'] == 'OVER':
            bet_type = f'for,tp,all,ahover,{int(line*4)}'
        elif bet_data['side'] == 'UNDER':
            bet_type = f'for,tp,all,ahunder,{int(line*4)}'
    
    if bet_data['bet_id'] == '7':
        if bet_data['side'] == 'DC_X2':
            bet_type = 'for,tp,all,dc,a,d'
        elif bet_data['side'] == 'DC_1X':
            bet_type = 'for,tp,all,dc,h,d'
        elif bet_data['side'] == 'DC_12':
            bet_type = 'for,tp,all,dc,h,a'
            
    return bet_type
def get_general_molly_bet_type(bet_data):
    line = bet_data['line']
    bet_type = None
    if bet_data['bet_id'] == '52':
        if bet_data['side'] == 'HOME':
            bet_type = 'for,ml,h'
        elif bet_data['side'] == 'AWAY':
            bet_type = 'for,ml,a'
            
    if bet_data['bet_id'] == '3':
        line = int(line * 4)
        if bet_data['side'] == 'HOME':
            bet_type = f'for,ah,h,{line}'
        elif bet_data['side'] == 'AWAY':
            bet_type = f'for,ah,a,{line *-1}'
    
    if bet_data['bet_id'] == '1':
        if bet_data['side'] == 'HOME':
            bet_type = f'for,h'
        elif bet_data['side'] == 'AWAY':
            bet_type = 'for,a'
        elif bet_data['side'] == 'DRAW':
            bet_type = 'for,d'
            
    if bet_data['bet_id'] == '2':
        line = int(line * 4)
        if bet_data['side'] == 'OVER':
            bet_type = f'for,ahover,{line}'
        elif bet_data['side'] == 'UNDER':
            bet_type = f'for,ahunder,{line}'
    
    if bet_data['bet_id'] == '7':
        if bet_data['side'] == 'DC_X2':
            bet_type = 'for,dc,a,d'
        elif bet_data['side'] == 'DC_1X':
            bet_type = 'for,dc,h,d'
        elif bet_data['side'] == 'DC_12':
            bet_type = 'for,dc,h,a'
            
    return bet_type



class WSStream(object):
    def __init__(self, s, tk):
        self.base_url = 'https://api.sportmarket.com'
        self.ws = None
        self.s = s
        self.token = tk
        self.offers_stream = []
        
        

    def start_stream(self):
        url = f'wss://api.mollybet.com/v1/stream/?token={self.token}'
        self.ws = websocket.WebSocketApp(url,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        # while True:
        try:
            self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}) # sslopt={"cert_reqs": ssl.CERT_NONE}
        except Exception as e:
            logging.error('error in run_forever() method: %s', e)
        # sleep(5)
    
    def register_event(self, event_id, sport):
        # print(f'registering to event_id: {event_id}')
        msg = f'["register_event", "{sport}", "{event_id}"]'
        self.ws.send(msg)
        
        
    def unregister_event(self, event_id, sport):
        print(f"Offers received for {event_id}, now unregistering event")
        msg = f'["unregister_event", "{sport}", "{event_id}"]'
        self.ws.send(msg)
        

    def on_open(self, ws):
        logging.info('Connection Opened')
        
    def close_stream(self):
        self.ws.close()
        logging.info('Connection Closed')
        
    def on_close(self, ws, code, msg):
        # print('stream connection closed')
        pass
        
    def on_error(self, ws, msg: str):
        # logging.error('Error: %s ', msg)
        print('Error: %s ', msg)

    def on_message(self, ws, msg: str):
        main_data = json.loads(msg) 
        data = main_data['data']
        for data in data:
            
            #Only add the valid events to the event_stream list
            if 'event' in data[0] and "ir_status" in data[1]:
                if data[1]['ir_status'] == 'pre_event' and data not in event_stream and any(x in data[1]['sport'] for x in valid_sports):
                    # print(data[1])
                    event_stream.append(data[1])
                    # stream = json.dumps(data[1])
                    # self.f.write(f'{stream}')
                    # self.f.write('\n')
                    
            # if websocket data is an offer, parse the valid bet_types offers into the offers_stream
            elif data[0] == 'offer': #and data[1]['bookie'] in offer_bookies
                stream = json.dumps(data[1])
                self.offers_stream.append(data[1])
                # self.off.write(f'{stream}')
                # self.off.write('\n')
    
    def clear_from_offer_stream(self, event_id):
        for offer in self.offers_stream:
            if event_id == offer['event_id']:
                self.offers_stream.remove(offer)
        logging.info(f'offers for {event_id} have been cleared')
        print(f'offers for {event_id} have been cleared')
    
    
def get_double_match_score(home:str, away:str, home_options: list, away_options: list):
    homeScore = process.extract(home, home_options, scorer=fuzz.token_sort_ratio, limit=1)
    awayScore = process.extract(away, away_options, scorer=fuzz.token_sort_ratio, limit=1)
    return homeScore[0][1], awayScore[0][1]

def get_single_match_score(search_txt:str, options: list):
    score = process.extract(search_txt, options, scorer=fuzz.token_sort_ratio, limit=1)
    return score[0][1]

def normalise_name(s):
    s = ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')
    s = s.replace('U21', 'Junior Team 21').replace('U20', 'Junior Team 20').replace('U23', 'Junior Team 23').replace('U19', 'Junior Team 19').replace('U18', 'Junior Team 18').replace('U17', 'Junior Team 17').replace('U16', 'Junior Team 16')
    return s

def get_saved_token():
    f = open('token.txt')
    lines = f.readlines()
    # print(lines)
    token = lines[0].strip()
    return token  

def get_pyckio_id():
    f = open('pyckio_user.txt')
    lines = f.readlines()
    # print(lines)
    id_ = lines[0].strip()
    return id_ 
    
    

if __name__ == "__main__":
    base_url = 'https://api.mollybet.com'
    event_stream = []
    entries = []
    valid_sports = ['fb', 'baseball', 'basket', 'tennis']
    
    #connect to google sheet
    gc = pygsheets.authorize(service_file='google_client.json')
    sht = gc.open_by_key('1NucB1OEjEEb93Q0PRNnPwzLcpmjzUow14NalVryWLgA')
    wks1 = sht.worksheet_by_title('Report')
    rows = wks1.get_all_values(include_tailing_empty=True, include_tailing_empty_rows=False, returnas='matrix')
    parsed_ids = [row[0] for row in rows[1:]]
    
    pyckio_user_id = get_pyckio_id()
    
    not_matched = []
    stake_amount = input('Enter stake amount to use >>>')
    stake_ = float(stake_amount)
    logging.info(">>> Launching Bot!!!")
    event_stream = []
    refresh_count = 0 
    client = SpMarketClient()
    counter = 0
    
    
    print('>>> Pyckio data stream now starting...')
    while True:
        # input('wait...')
        try:
            if refresh_count % 60  == 0:
                try:
                    session = requests.Session()
                    tn = get_saved_token()
                    bal = client.get_balance(session, tn)
                    if float(bal) < stake_:
                        print('-------------INSUFFICIENT FUNDS IN BETTING ACCOUNT-------------')
                        break
                    if bal is None:
                        session, tn = client.login()
                        bal = client.get_balance(session, tn)
                except:
                    logging.error('Error in reading login token', exc_info=True)
                    session, tn = client.login()
                    refresh_count = 0
            
            if refresh_count == 0:
                wss = WSStream(session, tn)
                t = Thread(target=wss.start_stream, name=f'threaddd_{refresh_count}')
                t.start()
            elif refresh_count % 300 == 0 and refresh_count != 0:
            # Start asynchronous socket stream
                wss.close_stream()
                event_stream.clear()
                sleep(1)
                wss = WSStream(session, tn)
                t = Thread(target=wss.start_stream, name=f'threaddd_{refresh_count}')
                t.start()
            
            #wait for all the latest event streams to come in before starting parse_bets function
            sleep(10)
            
            main()
            #after parsing all bets, close the connection stream
            
            refresh_count += 1
            counter = 1
            print('>>> Refreshing Pyckio data stream...')
        except KeyboardInterrupt:
            wss.close_stream()
            sys.exit(1)
        except Exception as e:
            logging.error(f'error in logic: ', exc_info=True)
            wss.close_stream()
            
    # wss.close_stream()