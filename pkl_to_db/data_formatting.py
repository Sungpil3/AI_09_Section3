# data를 db에 넣기 좋게 구조화
import pickle
import json
import os
for file in ['formatted_data.pkl', 'columns.pkl']:
    if os.path.exists(file):
        os.remove(file)

user_match_list = None
with open('../api_to_pkl/user_match_list.pkl','rb') as pickle_file:
   user_match_list = pickle.load(pickle_file)
new_list = []
for user in user_match_list:
    name = user['nickName']
    for match_type in user['matches']:
        for i in match_type['matches']:
            i['nickName'] = name 
            new_list.append(i)
user_match_list = new_list
new_list = None

# player 내부의 데이터를 꺼내기
redundent_keys = set(user_match_list[0].keys()).intersection(set(user_match_list[0]['player'].keys()))
for match in user_match_list:
    for k in match['player'].keys():
        if not k in redundent_keys:
            match[k] = match['player'][k]
    del match['player']

metadata = {}
files = ['character', 'flyingPet', 'matchType', 'kart', 'pet', 'trackId', 'channelName' , 'partsEngine', 'partsHandle', 'partsKit', 'partsWheel']
for filename in files:
    with open(f'../metadata/{filename}.json', "r", encoding='UTF8') as st_json:
        json_data = json.load(st_json)
    metadata_value = {}
    for i in json_data:
        id = i['id']
        name = i['name']
        metadata_value[id] = name
    metadata[filename] = metadata_value      

for match in user_match_list:
    for key in files:
        try:
            match[key] = metadata[key][match[key]]        
        except:
            match[key] = ''

with open('formatted_data.pkl','wb') as pickle_file:
    pickle.dump(user_match_list, pickle_file)

dict_for_create_table = dict()
dict_for_create_table['kart_user'] = ['accountNo', 'nickName']
dict_for_create_table['kart_match'] = ['matchId', 'matchType', 'startTime', 'endTime', 'channelName', 'trackId', 'playerCount']
dict_for_create_table['kart_user_match'] = ['accountNo', 'matchId', 'kart', 'license', 'pet', 'flyingPet', 'partsEngine', 'partsHandle', 'partsWheel', 'partsKit', 'rankinggrade2', 'matchRank', 'matchRetired', 'matchWin', 'matchTime']

with open('columns.pkl','wb') as pickle_file:
    pickle.dump(dict_for_create_table, pickle_file)