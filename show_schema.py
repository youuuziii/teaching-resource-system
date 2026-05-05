import os
from dotenv import load_dotenv
import pymysql

load_dotenv()

db_url = os.getenv('DATABASE_URL').replace('mysql+pymysql://', '')
user_pass, rest = db_url.split('@')
user, password = user_pass.split(':')
host_port, db_params = rest.split('/')
if ':' in host_port:
    host, port = host_port.split(':')
    port = int(port)
else:
    host = host_port
    port = 3306
db = db_params.split('?')[0]

conn = pymysql.connect(
    host=host, 
    port=port, 
    user=user, 
    password=password, 
    database=db
)
cursor = conn.cursor()

def show_create_table(table_name):
    print(f"\n--- CREATE TABLE {table_name} ---")
    try:
        cursor.execute(f"SHOW CREATE TABLE {table_name}")
        print(cursor.fetchone()[1])
    except Exception as e:
        print(f"Error: {e}")

cursor.execute("SHOW TABLES")
tables = [row[0] for row in cursor.fetchall()]
for table in tables:
    show_create_table(table)

conn.close()
