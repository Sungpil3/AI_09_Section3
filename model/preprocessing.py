# 클러스터링을 위해 user별로 data를 전처리해서 저장
import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats
import seaborn as sns
import math
from datetime import datetime
# 모든 변수가 보이도록 출력
pd.options.display.max_columns = None 

# 그래프 출력 설정
plt.rc('font', family='Malgun Gothic')
plt.rcParams['axes.unicode_minus'] = False
# data 불러오기
df = None
with open('data.pkl','rb') as pickle_file:
   df = pickle.load(pickle_file)
df = pd.DataFrame(df)
print("data 불러오기 완료")

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
print("data 전처리 완료")

match_type_list = list(df['matchType'].value_counts().index)
kartEngine_list = list(df['kartEngine'].value_counts().index)
user_data_list = []
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
    user_data_list.append(user_dict)
    print(f"{len(data)}중 {count+1}완료")

with open('user_data.pkl','wb') as pickle_file:
    pickle.dump(user_data_list, pickle_file)    
