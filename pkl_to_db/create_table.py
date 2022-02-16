# database 의 table를 생성한다.
import pickle
columns = None
with open('columns.pkl','rb') as pickle_file:
   columns = pickle.load(pickle_file)

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

cur = connection.cursor()

table_name = 'kart_user'
cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
cur.execute(f"""
CREATE TABLE {table_name} (
{columns[table_name][0]} INTEGER NOT NULL PRIMARY KEY,
{columns[table_name][1]} VARCHAR(16)
);"""
)

table_name = 'kart_match'
cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
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
cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
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

connection.commit()
cur.close()
connection.close()
print(connection)