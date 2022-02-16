# metabase를 위해 sqlite에 데이터를 저장
import pickle
import sqlite3

connection = sqlite3.connect('meta.db')

user_match_list = None
with open('formatted_data.pkl','rb') as pickle_file:
   user_match_list = pickle.load(pickle_file)

columns = None
with open('columns.pkl','rb') as pickle_file:
   columns = pickle.load(pickle_file)

cur = connection.cursor()

table_name = 'kart_user'
cur.execute(f"DROP TABLE IF EXISTS {table_name};")
cur.execute(f"""
CREATE TABLE {table_name} (
{columns[table_name][0]} INTEGER NOT NULL PRIMARY KEY,
{columns[table_name][1]} VARCHAR(16)
);"""
)

table_name = 'kart_match'
cur.execute(f"DROP TABLE IF EXISTS {table_name};")
cur.execute(f"""
CREATE TABLE {table_name} (
{columns[table_name][0]} VARCHAR(32) NOT NULL PRIMARY KEY,     
{columns[table_name][1]} VARCHAR(32),
{columns[table_name][2]} VARCHAR(32),
{columns[table_name][3]} VARCHAR(32),
{columns[table_name][4]} VARCHAR(32),
{columns[table_name][5]} VARCHAR(32),
{columns[table_name][6]} INTEGER
);"""
)

table_name = 'kart_user_match'
cur.execute(f"DROP TABLE IF EXISTS {table_name};")
cur.execute(f"""
CREATE TABLE {table_name} (
{columns[table_name][0]} INTEGER NOT NULL,     
{columns[table_name][1]} VARCHAR(32) NOT NULL,
{columns[table_name][2]} VARCHAR(32),
{columns[table_name][3]} VARCHAR(32),
{columns[table_name][4]} VARCHAR(32),
{columns[table_name][5]} VARCHAR(32),
{columns[table_name][6]} VARCHAR(32),
{columns[table_name][7]} VARCHAR(32),
{columns[table_name][8]} VARCHAR(32),
{columns[table_name][9]} VARCHAR(32),
{columns[table_name][10]} VARCHAR(32),
{columns[table_name][11]} VARCHAR(32),
{columns[table_name][12]} VARCHAR(32),
{columns[table_name][13]} VARCHAR(32),
{columns[table_name][14]} VARCHAR(32),
PRIMARY KEY({columns[table_name][0]}, {columns[table_name][1]}),
CONSTRAINT kart_user FOREIGN KEY({columns[table_name][0]}) REFERENCES kart_user({columns[table_name][0]}),
CONSTRAINT kart_match FOREIGN KEY({columns[table_name][1]}) REFERENCES kart_match({columns[table_name][1]})
);"""
)

table_name = 'kart_user'

user_list = []
for match in user_match_list:
    user_list.append((int(match['accountNo']), match['nickName']))
user_list = set(user_list)

cur = connection.cursor()
cur.execute(f"DELETE FROM {table_name};")
for user in user_list:
    cur.execute(f"INSERT INTO {table_name} ({columns[table_name][0]}, {columns[table_name][1]}) VALUES ({user[0]}, '{user[1]}');")

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

table_name = 'kart_user_match'

match_list = []
match_set = set()
for match in user_match_list:
    if not (match[f'{columns[table_name][0]}'] , match[f'{columns[table_name][1]}']) in match_set:
        match_list.append((
                int(match[f'{columns[table_name][0]}']),
                match[f'{columns[table_name][1]}'], 
                match[f'{columns[table_name][2]}'], 
                match[f'{columns[table_name][3]}'], 
                match[f'{columns[table_name][4]}'], 
                match[f'{columns[table_name][5]}'], 
                match[f'{columns[table_name][6]}'],
                match[f'{columns[table_name][7]}'],
                match[f'{columns[table_name][8]}'],
                match[f'{columns[table_name][9]}'],
                match[f'{columns[table_name][10]}'],
                match[f'{columns[table_name][11]}'],
                match[f'{columns[table_name][12]}'],
                match[f'{columns[table_name][13]}'],
                match[f'{columns[table_name][14]}'],
                ))
    else:
        pass
    match_set.add((match[f'{columns[table_name][0]}'] , match[f'{columns[table_name][1]}']))
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
    {columns[table_name][6]},
    {columns[table_name][7]},
    {columns[table_name][8]},
    {columns[table_name][9]},
    {columns[table_name][10]},
    {columns[table_name][11]},
    {columns[table_name][12]},
    {columns[table_name][13]},
    {columns[table_name][14]}) 
    VALUES ({match[0]}, 
    '{match[1]}',
    '{match[2]}',
    '{match[3]}',
    '{match[4]}',
    '{match[5]}',
    '{match[6]}',
    '{match[7]}',
    '{match[8]}',
    '{match[9]}',
    '{match[10]}',
    '{match[11]}',
    '{match[12]}',
    '{match[13]}',
    '{match[14]}');""")    

connection.commit()
cur.close()
connection.close()
