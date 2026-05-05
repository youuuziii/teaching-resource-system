
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    db_url = os.getenv("DATABASE_URL")
    # mysql+pymysql://root:password@localhost:3306/teaching_system
    db_url = db_url.replace("mysql+pymysql://", "")
    user_pass, host_db = db_url.split("@")
    user, password = user_pass.split(":")
    host_db = host_db.split("?")[0]
    host_port, db_name = host_db.split("/")
    host, port = host_port.split(":")
    
    return pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=db_name
    )

conn = get_connection()
cursor = conn.cursor()

def inspect_table(table_name):
    print(f"\n--- Inspecting {table_name} ---")
    try:
        cursor.execute(f"DESCRIBE {table_name}")
        for row in cursor.fetchall():
            print(row)
    except Exception as e:
        print(f"Error inspecting {table_name}: {e}")

inspect_table("user_behaviors")
inspect_table("notifications")
inspect_table("users")

conn.close()
