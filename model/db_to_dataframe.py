# db에서 data 가져오기
import pickle
import psycopg2
host = 'localhost'
user = 'postgres'
password = '로컬 DB 비밀번호'
database = 'kart'

columns = None
with open('../pkl_to_db/columns.pkl','rb') as pickle_file:
   columns = pickle.load(pickle_file)
columns = columns['kart_user_match'] + columns['kart_user'] +columns['kart_match']

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cur = connection.cursor()

cur.execute("SELECT * FROM kart_user_match kum FULL OUTER JOIN kart_user ku on kum.accountno = ku.accountno FULL outer join kart_match km on kum.matchid = km.matchid;")
rows = cur.fetchall()
df = []
for row in rows:
    row_data = dict()
    for i, column in enumerate(columns): 
        row_data[column] = row[i]
    df.append(row_data)        

cur.close()
connection.close()

with open('data.pkl','wb') as pickle_file:
    pickle.dump(df, pickle_file)