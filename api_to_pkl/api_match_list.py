# api로 2022년 2월 10일 15시 13분 22초를 기준으로 1000시간 전 까지 1시간에 200개의 match_id를 set으로 가져와서 부호화
# match_list_set.pkl은 match_id의 set
import requests
import json
import os
import time
import datetime
import pickle
API_Key = '<API_Key를 입력해주세요>'
headers = {'Authorization': API_Key}
now = datetime.datetime(2022, 2, 10, 15, 13, 22)
match_list = []
for i in range(1, 1001):
    start_date = str(now - datetime.timedelta(hours=i))
    end_date= str(now - datetime.timedelta(hours=i-1))
    url = 'https://api.nexon.co.kr/kart/v1.0/matches/all?start_date='+ start_date +'&end_date='+ end_date +'&offset=0&limit=200&match_types='
    json_data = requests.get(url, headers=headers)
    match_data = json.loads(json_data.text)
    if match_data['matches'] == None:
        print(f"{start_date} 부터 {end_date} 사이 에는 match data 가 없습니다.")
    else :
        for i in range(len(match_data['matches'])):
            match_list += match_data['matches'][i]['matches']
    time.sleep(0.5)
match_list = set(match_list)
with open('match_list_set.pkl','wb') as pickle_file:
    pickle.dump(match_list, pickle_file)        
