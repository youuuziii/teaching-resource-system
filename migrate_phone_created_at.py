import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

def migrate():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL not found in environment")
        return

    db_url = db_url.replace("mysql+pymysql://", "")
    user_pass, host_db = db_url.split("@")
    user, password = user_pass.split(":")
    host_db = host_db.split("?")[0]
    host_port, db_name = host_db.split("/")
    if ":" in host_port:
        host, port = host_port.split(":")
        port = int(port)
    else:
        host = host_port
        port = 3306

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db_name,
        autocommit=True
    )
    cursor = conn.cursor()

    try:
        # 1. 在 students, teachers, deans 表中加入 phone 属性
        print("Adding phone column to students, teachers, deans...")
        cursor.execute("ALTER TABLE students ADD COLUMN phone VARCHAR(32) DEFAULT NULL")
        cursor.execute("ALTER TABLE teachers ADD COLUMN phone VARCHAR(32) DEFAULT NULL")
        cursor.execute("ALTER TABLE deans ADD COLUMN phone VARCHAR(32) DEFAULT NULL")

        # 2. 将 users 表中的 phone 数据迁移到对应的角色表中
        print("Migrating phone data from users to role tables...")
        # 迁移学生手机号
        cursor.execute("""
            UPDATE students s 
            JOIN users u ON s.user_id = u.id 
            SET s.phone = u.phone 
            WHERE u.phone IS NOT NULL
        """)
        # 迁移教师手机号
        cursor.execute("""
            UPDATE teachers t 
            JOIN users u ON t.user_id = u.id 
            SET t.phone = u.phone 
            WHERE u.phone IS NOT NULL
        """)
        # 迁移教务手机号
        cursor.execute("""
            UPDATE deans d 
            JOIN users u ON d.user_id = u.id 
            SET d.phone = u.phone 
            WHERE u.phone IS NOT NULL
        """)

        # 3. 删除 users 表中的 phone 属性
        print("Dropping phone column from users...")
        cursor.execute("ALTER TABLE users DROP COLUMN phone")

        # 4. 删除 students, teachers, deans 表中的 created_at 属性
        print("Dropping created_at column from students, teachers, deans...")
        cursor.execute("ALTER TABLE students DROP COLUMN created_at")
        cursor.execute("ALTER TABLE teachers DROP COLUMN created_at")
        cursor.execute("ALTER TABLE deans DROP COLUMN created_at")

        print("Migration completed successfully!")

    except Exception as e:
        print(f"Migration failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrate()
