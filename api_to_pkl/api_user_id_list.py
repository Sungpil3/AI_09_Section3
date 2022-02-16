# match_list_set.pkl의 match_id중 랜덤하게 10000개를 이용해서 해당 match 의 user 들의 accountNo 를 부호화
# user_id_list_set.pkl는 user들의 accountNo로 이루어진 set
import pickle
import requests
import json
import random
import time
API_Key = '<API_Key를 입력해주세요>'
headers = {'Authorization': API_Key}
macth_list = None
with open('match_list_set.pkl','rb') as pickle_file:
   macth_list = pickle.load(pickle_file)
macth_list = list(macth_list)
random.shuffle(macth_list)
macth_list = macth_list[:10000]
user_id_list = set()
for i in macth_list:
    url = 'https://api.nexon.co.kr/kart/v1.0/matches/'+ i
    json_data = requests.get(url, headers=headers)
    match_data = json.loads(json_data.text)
    #print(match_data)
    if 'players' in match_data:
        for ii in range(len(match_data['players'])):
            user_id_list.add(match_data['players'][ii]['accountNo'])
    else :
        for teams in range(len(match_data['teams'])):
            for iii in range(len(match_data['teams'][teams]['players'])):                
                user_id_list.add(match_data['teams'][teams]['players'][iii]['accountNo'])
    time.sleep(0.5)    
print(len(user_id_list))
with open('user_id_list_set.pkl','wb') as pickle_file:
    pickle.dump(user_id_list, pickle_file)
