# db에 data 적재
import pickle
import psycopg2
host = 'localhost'
user = 'postgres'
password = '로컬 DB 비밀번호'
database = 'kart'

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

user_match_list = None
with open('formatted_data.pkl','rb') as pickle_file:
   user_match_list = pickle.load(pickle_file)

columns = None
with open('columns.pkl','rb') as pickle_file:
   columns = pickle.load(pickle_file)

table_name = 'kart_match'

match_list = []
match_set = set()
for match in user_match_list:
    if not match[f'{columns[table_name][0]}'] in match_set:
        match_list.append((
                match[f'{columns[table_name][0]}'],
                match[f'{columns[table_name][1]}'], 
                match[f'{columns[table_name][2]}'], 
                match[f'{columns[table_name][3]}'], 
                match[f'{columns[table_name][4]}'], 
                match[f'{columns[table_name][5]}'], 
                int(match[f'{columns[table_name][6]}'])))
    else:
        pass
    match_set.add(match[f'{columns[table_name][0]}'])
match_list = set(match_list)

cur = connection.cursor()
cur.execute(f"DELETE FROM {table_name};")
for match in match_list:
    cur.execute(f"""INSERT INTO {table_name} 
    ({columns[table_name][0]}, 
    {columns[table_name][1]},
    {columns[table_name][2]},
    {columns[table_name][3]},
    {columns[table_name][4]},
    {columns[table_name][5]},
    {columns[table_name][6]}) 
    VALUES ('{match[0]}', 
    '{match[1]}',
    '{match[2]}',
    '{match[3]}',
    '{match[4]}',
    '{match[5]}',
    {match[6]});""")

connection.commit()
cur.close()
connection.close()
