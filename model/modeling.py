# 스케일러 객체와 모델 객체를 부호화
import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
df = None
with open('user_data.pkl','rb') as pickle_file:
   df = pickle.load(pickle_file)
df = pd.DataFrame(df)
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
name = "all"
col = list(df.columns)
scaler = StandardScaler()
standarzed_df = scaler.fit_transform(df)
standarzed_df = pd.DataFrame(standarzed_df)
standarzed_df.columns = col
kmeans = KMeans(n_clusters = 6, random_state=42)
kmeans.fit(standarzed_df)

with open('scaler.pkl','wb') as pickle_file:
    pickle.dump(scaler, pickle_file)

with open('model.pkl','wb') as pickle_file:
    pickle.dump(kmeans, pickle_file)