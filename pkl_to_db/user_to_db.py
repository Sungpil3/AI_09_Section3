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

table_name = 'kart_user'

user_list = []
for match in user_match_list:
    user_list.append((int(match['accountNo']), match['nickName']))
user_list = set(user_list)

cur = connection.cursor()
cur.execute(f"DELETE FROM {table_name};")
for user in user_list:
    cur.execute(f"INSERT INTO {table_name} ({columns[table_name][0]}, {columns[table_name][1]}) VALUES ({user[0]}, '{user[1]}');")

connection.commit()
cur.close()
connection.close()

