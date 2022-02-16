from flask import Blueprint
import requests
import json
import os
import time
import datetime
import pickle
API_Key = '<API_Key를 입력해주세요>'
headers = {'Authorization': API_Key}
bp = Blueprint('result', __name__, url_prefix='/result')

@bp.route('/<user_name>')
def index(user_name):
    url = f'https://api.nexon.co.kr/kart/v1.0/users/nickname/{user_name}'
    json_data = requests.get(url, headers=headers)
    user = json.loads(json_data.text)
    try :
        accountNo = user['accessId']
    except:
        return "해당 유저의 data가 없습니다.\nData based on NEXON DEVELOPERS"
    url = 'https://api.nexon.co.kr/kart/v1.0/users/'+ accountNo +'/matches?start_date=&end_date= &offset=0&limit=200&match_types='
    json_data = requests.get(url, headers=headers)
    match_data = json.loads(json_data.text)
    new_list = []
    name = match_data['nickName']
    for match_type in match_data['matches']:
        for i in match_type['matches']: 
            new_list.append(i)
    user_match_list = new_list
    del new_list

    redundent_keys = set(user_match_list[0].keys()).intersection(set(user_match_list[0]['player'].keys()))
    for match in user_match_list:
        for k in match['player'].keys():
            if not k in redundent_keys:
                match[k] = match['player'][k]
        del match['player']
    metadata = {}
    files = ['character', 'flyingPet', 'matchType', 'kart', 'pet', 'trackId', 'channelName' , 'partsEngine', 'partsHandle', 'partsKit', 'partsWheel']
    for filename in files:
        with open(f'{filename}.json', "r", encoding='UTF8') as st_json:
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
    import pandas as pd
    df = pd.DataFrame(user_match_list)
    df['nickName'] = pd.Series([f"{user_name}"]*200)
    df = df[['accountNo', 'matchId', 'kart', 'license', 'pet', 'flyingPet',
        'partsEngine', 'partsHandle', 'partsWheel', 'partsKit', 'rankinggrade2',
        'matchRank', 'matchRetired', 'matchWin', 'matchTime', 'nickName',
        'matchType', 'startTime', 'endTime', 'channelName', 'trackId',
        'playerCount']]
    import numpy as np
    import matplotlib.pyplot as plt
    import scipy.stats as stats
    import seaborn as sns
    import math
    from datetime import datetime
    df = df.drop(columns=['license', 'pet', 'partsEngine', 'partsHandle', 'partsWheel', 'partsKit', 'matchTime', 'nickName', 'endTime', 'matchWin','flyingPet'])
    def other(x):
        if x == "":
            return "기타"
        else:
            return x
    def to_engine(kart):
        try :
            engine = kart.split()[-1]
        except :
            engine = "기타"
        return engine
    df['kartEngine'] = df['kart'].apply(to_engine)
    last = set(df['kartEngine'].value_counts(normalize=True)[4:].index)
    def pre_engine(engine):
        if engine in last:
            return "기타"
        else :
            return engine
    df["kartEngine"] = df["kartEngine"].apply(pre_engine)
    del last
    df = df[df['rankinggrade2'] != ""]
    df['rankinggrade2'] = df['rankinggrade2'].apply(lambda x : int(x))
    def for_matchRank(matchRank):
        try:
            return int(matchRank)
        except:
            return 99 
    df["matchRank"] = df["matchRank"].apply(for_matchRank)
    def for_retire(matchRank):
        if matchRank == 99:
            return 1
        else:
            return 0    
    df["matchRetired"] = df["matchRank"].apply(for_retire)

    df['channelName'] = df['channelName'].apply(other)
    def other2(matchType):
        matchType = matchType.replace("(", " ").replace(")", "").split()
        if (("무한" in matchType) or ("무한부스터" in matchType)) and ("개인전" in matchType):
            return "무부 개인전"
        elif (("무한" in matchType) or ("무한부스터" in matchType)) and ("팀전" in matchType):
            return "무부 팀전"
        elif not (("무한" in matchType) or ("무한부스터" in matchType)) and (("스피드" in matchType) and (("팀전" in matchType) or ("클럽" in matchType))):
            return "스피드 팀전"
        elif not (("무한" in matchType) or ("무한부스터" in matchType)) and (("스피드" in matchType) and ("개인전" in matchType)):
            return "스피드 개인전"
        elif ("아이템" in matchType) and (("팀전" in matchType) or ("클럽" in matchType)):
            return "아이템 팀전"
        elif ("아이템" in matchType) and ("개인전" in matchType):
            return "아이템 개인전"
        else :
            return "기타"
    df['matchType'] = df['channelName'].apply(other2)
    df = df.drop('channelName', axis=1)

    v1 = datetime(2021, 3, 4, 0, 0)
    def time_format(time):
        return (datetime.strptime(time.split("T")[0] , "%Y-%m-%d") - v1).days
    df['v1_days'] = df['startTime'].apply(time_format)
    del v1
    df = df.drop('startTime', axis=1)

    df['trackId'] = df['trackId'].apply(other)
    def track_type(track):
        return track.split()[0]
    df['tracktype'] = df['trackId'].apply(track_type)
    others = set(df['tracktype'].value_counts()[35:].index)
    def pre_track(track):
        if track in others:
            return "기타"
        elif track in ["[reverse]", "[R]"]:
            return "리버스"    
        else :
            return track
    df['tracktype'] = df['tracktype'].apply(pre_track)  

    df['kart'] = df['kart'].apply(other)

    license_dict = {
        0 : "없음",
        1 : "초보",
        2 : "루키",
        3 : "L3",
        4 : "L2",
        5 : "L1",
        6 : "Pro"
    }
    def for_license(a):
        return license_dict[a]
    df['license'] = df['rankinggrade2'].apply(for_license)

    match_type_list = ['스피드 팀전', '아이템 팀전', '무부 팀전', '스피드 개인전', '아이템 개인전', '무부 개인전', '기타']
    kartEngine_list = ['X', 'V1', '기타', '9']
    data = set(df['accountNo'])
    for count, accountNo in enumerate(data):
        user_dict = dict()
        user_df = df[df['accountNo'] == accountNo]
        user_dict['accountNo'] = accountNo
        for i in kartEngine_list:
            if i in user_df['kartEngine'].value_counts(normalize=True):
                user_dict[f"kartEngine_ratio_{i}"] = user_df['kartEngine'].value_counts(normalize=True)[i]
            else:
                user_dict[f"kartEngine_ratio_{i}"] = 0     
        user_dict["kart_mode"] = user_df['kart'].value_counts(normalize=True).index[0]
        user_dict["kart_mode_ratio"] =  user_df['kart'].value_counts(normalize=True)[0]
        user_dict["kartEngine_mode"] = user_df['kartEngine'].value_counts(normalize=True).index[0]
        user_dict["kartEngine_mode_ratio"] =  user_df['kartEngine'].value_counts(normalize=True)[0]
        user_dict['license'] = license_dict[user_df['rankinggrade2'].max()]
        user_dict['matchRank_maen'] = user_df[user_df['matchRank'] != 99]['matchRank'].mean()
        user_dict['matchRetired_ratio'] = user_df['matchRetired'].sum() / df[df['accountNo'] == accountNo].shape[0]
        for i in match_type_list:
            if i in user_df['matchType'].value_counts(normalize=True):
                user_dict[f"matchType_ratio_{i}"] = user_df['matchType'].value_counts(normalize=True)[i]
            else:
                user_dict[f"matchType_ratio_{i}"] = 0
        user_dict["track_mode"] = user_df['trackId'].value_counts(normalize=True).index[0]
        user_dict["track_mode_ratio"] =  user_df['trackId'].value_counts(normalize=True)[0]
        user_dict["track_type_mode"] = user_df['tracktype'].value_counts(normalize=True).index[0]
        user_dict["track_type_mode_ratio"] =  user_df['tracktype'].value_counts(normalize=True)[0]
        user_dict["playerCount_mode"] = user_df['playerCount'].value_counts(normalize=True).index[0]
        user_dict["playerCount_mode_ratio"] =  list(user_df['playerCount'].value_counts(normalize=True))[0]
    df = pd.DataFrame([user_dict])
    df = df.dropna()
    a = []
    for i in list(df.columns):
        if i.split("_")[-1] == "mode":
            a.append(i)
    a = a[:-1]        
    a.append('license')        
    df = df.drop(columns=a)
    df = df.drop('accountNo', axis=1)
    df = df.reset_index(drop=True)
    col = list(df.columns)
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    model = None
    with open('model.pkl','rb') as pickle_file:
        model = pickle.load(pickle_file)
    scaler = None   
    with open('scaler.pkl','rb') as pickle_file:
        scaler = pickle.load(pickle_file)
    sample = scaler.transform(df)
    sample = pd.DataFrame(sample)
    sample.columns = col
    result = model.predict(sample)[0]
    result_dict = {
        0: "v1 엔진을 선호하는 스피드 팀전 유저",

        1 :"아이템 개인전 유저",

        2 :"v1 엔진을 선호하는 무한 부스터 모드 유저, 빌리지 운하매니아",

        3 :"무한부스터 개인전, 기타 모드 유저",

        4 :"스피드 개인전 유저",

        5 :"아이템 팀전 유저 풀방 매니아"
    }
    return f"{user_name}는 {result_dict[result]}입니다.\nData based on NEXON DEVELOPERS"
