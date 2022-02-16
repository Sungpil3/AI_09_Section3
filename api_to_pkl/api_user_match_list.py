# user_id_list_set.pkl의 accountNo를 이용해서 각 유저별 200개의 match data를 부호화
# user_match_data.pkl은 user의 match data(dict)로 이루어진 list
import pickle
import requests
import json
import time
API_Key = '<API_Key를 입력해주세요>'
headers = {'Authorization': API_Key}
macth_list = None
with open('user_id_list_set.pkl','rb') as pickle_file:
   user_id_list = pickle.load(pickle_file)

user_match_data = []
for i, user_id in enumerate(user_id_list):
    url = 'https://api.nexon.co.kr/kart/v1.0/users/'+ user_id +'/matches?start_date=&end_date= &offset=0&limit=200&match_types='
    json_data = requests.get(url, headers=headers)
    match_data = json.loads(json_data.text)
    user_match_data.append(match_data)
    print(f"{len(user_id_list)}번중 {i+1}번째 완료")
    time.sleep(0.5)    

with open('user_match_data.pkl','wb') as pickle_file:
    pickle.dump(user_match_data, pickle_file)

