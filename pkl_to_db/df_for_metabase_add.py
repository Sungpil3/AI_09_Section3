import pickle
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
df = None
with open('../model/user_data.pkl','rb') as pickle_file:
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
labels = kmeans.labels_
all_label = pd.DataFrame({name:labels})
with open('../model/user_data.pkl','rb') as pickle_file:
   df = pickle.load(pickle_file)
df = pd.DataFrame(df)
df = df.dropna()
df = df.reset_index(drop=True)
df = pd.concat([df, all_label], axis=1)
columns = list(df.columns)
columns2 = []
for col in columns:
    columns2.append(col.replace(" ", "_"))
tp = []
for i in columns:
    tp.append(str(df.dtypes[i]))
dict_for_add={
    'int64':'INTEGER',
    'float64':'REAL',
    'int32':'INTEGER',
    'object':'VARCHAR(32)'
}

import sqlite3

connection = sqlite3.connect('meta.db')
cur = connection.cursor()

table_name = 'kart_user_label'
cur.execute(f"DROP TABLE IF EXISTS {table_name};")
cur.execute(f"""
CREATE TABLE {table_name} (
{columns2[0]} {dict_for_add[tp[0]]} NOT NULL PRIMARY KEY,
{columns2[1]} {dict_for_add[tp[1]]},
{columns2[2]} {dict_for_add[tp[2]]},
{columns2[3]} {dict_for_add[tp[3]]},
{columns2[4]} {dict_for_add[tp[4]]},
{columns2[5]} {dict_for_add[tp[5]]},
{columns2[6]} {dict_for_add[tp[6]]},
{columns2[7]} {dict_for_add[tp[7]]},
{columns2[8]} {dict_for_add[tp[8]]},
{columns2[9]} {dict_for_add[tp[9]]},
{columns2[10]} {dict_for_add[tp[10]]},
{columns2[11]} {dict_for_add[tp[11]]},
{columns2[12]} {dict_for_add[tp[12]]},
{columns2[13]} {dict_for_add[tp[13]]},
{columns2[14]} {dict_for_add[tp[14]]},
{columns2[15]} {dict_for_add[tp[15]]},
{columns2[16]} {dict_for_add[tp[16]]},
{columns2[17]} {dict_for_add[tp[17]]},
{columns2[18]} {dict_for_add[tp[18]]},
{columns2[19]} {dict_for_add[tp[19]]},
{columns2[20]} {dict_for_add[tp[20]]},
{columns2[21]} {dict_for_add[tp[21]]},
{columns2[22]} {dict_for_add[tp[22]]},
{columns2[23]} {dict_for_add[tp[23]]},
{columns2[24]} {dict_for_add[tp[24]]},
label {dict_for_add[tp[25]]},
CONSTRAINT kart_user_label FOREIGN KEY({columns[0]}) REFERENCES kart_user({columns[0]})
);"""
)
user_label_list = df.to_dict('records')
for user_label in user_label_list:
    cur.execute(f"""INSERT INTO {table_name} 
    ({columns2[0]}, 
    {columns2[1]},
    {columns2[2]},
    {columns2[3]},
    {columns2[4]},
    {columns2[5]},
    {columns2[6]},
    {columns2[7]},
    {columns2[8]},
    {columns2[9]},
    {columns2[10]},
    {columns2[11]},
    {columns2[12]},
    {columns2[13]},
    {columns2[14]},
    {columns2[15]},
    {columns2[16]},
    {columns2[17]},
    {columns2[18]},
    {columns2[19]},
    {columns2[20]},
    {columns2[21]},
    {columns2[22]},
    {columns2[23]},
    {columns2[24]},
    label) 
    VALUES ({user_label[columns[0]]}, 
    {user_label[columns[1]]},
    {user_label[columns[2]]},
    {user_label[columns[3]]},
    {user_label[columns[4]]},
    '{user_label[columns[5]]}',
    {user_label[columns[6]]},
    '{user_label[columns[7]]}',
    {user_label[columns[8]]},
    '{user_label[columns[9]]}',
    {user_label[columns[10]]},
    {user_label[columns[11]]},
    {user_label[columns[12]]},
    {user_label[columns[13]]},
    {user_label[columns[14]]},
    {user_label[columns[15]]},
    {user_label[columns[16]]},
    {user_label[columns[17]]},
    {user_label[columns[18]]},
    '{user_label[columns[19]]}',
    {user_label[columns[20]]},
    '{user_label[columns[21]]}',
    {user_label[columns[22]]},
    {user_label[columns[23]]},
    {user_label[columns[24]]},
    {user_label[columns[25]]}
    );""")


connection.commit()
cur.close()
connection.close()