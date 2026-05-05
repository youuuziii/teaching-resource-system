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

def drop_foreign_keys_to_table(table_name, target_table):
    cursor.execute(f"""
        SELECT CONSTRAINT_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = '{db}' 
          AND TABLE_NAME = '{table_name}' 
          AND REFERENCED_TABLE_NAME = '{target_table}'
    """)
    for (constraint_name,) in cursor.fetchall():
        try:
            cursor.execute(f"ALTER TABLE {table_name} DROP FOREIGN KEY {constraint_name}")
            print(f"Dropped foreign key {constraint_name} on {table_name}")
        except Exception as e:
            print(f"Failed to drop foreign key {constraint_name}: {e}")

try:
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

    # 1. Delete the first two default records in teachers table
    cursor.execute("SELECT id FROM teachers ORDER BY id ASC LIMIT 2")
    to_delete = cursor.fetchall()
    for (tid,) in to_delete:
        # Before deleting teacher, delete related records in course_teachers and resource_teachers
        cursor.execute("DELETE FROM course_teachers WHERE teacher_id = %s", (tid,))
        cursor.execute("DELETE FROM resource_teachers WHERE teacher_id = %s", (tid,))
        cursor.execute("DELETE FROM teachers WHERE id = %s", (tid,))
        print(f"Deleted teacher with id {tid} and related records")

    # 2. Reformat teacher_id to t01, t02, etc.
    cursor.execute("SELECT id FROM teachers ORDER BY id ASC")
    teachers = cursor.fetchall()
    for index, (tid,) in enumerate(teachers):
        cursor.execute("UPDATE teachers SET teacher_id = %s WHERE id = %s", (f"temp_{index}", tid))
    
    for index, (tid,) in enumerate(teachers):
        new_teacher_id = f"t{index + 1:02d}"
        cursor.execute("UPDATE teachers SET teacher_id = %s WHERE id = %s", (new_teacher_id, tid))
        print(f"Updated teacher id {tid} with teacher_id {new_teacher_id}")

    # 3. Handle schema changes for teachers
    # Prepare new teacher_id columns in related tables
    for tbl in ["course_teachers", "resource_teachers"]:
        cursor.execute(f"SHOW COLUMNS FROM {tbl} LIKE 'teacher_id_new'")
        if not cursor.fetchone():
            cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN teacher_id_new VARCHAR(64)")
        cursor.execute(f"UPDATE {tbl} child JOIN teachers t ON child.teacher_id = t.id SET child.teacher_id_new = t.teacher_id")

    # Drop foreign keys referencing teachers.id
    drop_foreign_keys_to_table("course_teachers", "teachers")
    drop_foreign_keys_to_table("resource_teachers", "teachers")

    # For teachers:
    cursor.execute("SHOW COLUMNS FROM teachers LIKE 'id'")
    if cursor.fetchone():
        cursor.execute("ALTER TABLE teachers MODIFY COLUMN id INT") # Remove auto_increment
        cursor.execute("ALTER TABLE teachers DROP PRIMARY KEY")
        cursor.execute("ALTER TABLE teachers DROP COLUMN id")
        cursor.execute("ALTER TABLE teachers ADD PRIMARY KEY (teacher_id)")
    
    # For course_teachers:
    cursor.execute("SHOW COLUMNS FROM course_teachers LIKE 'teacher_id'")
    if cursor.fetchone():
        cursor.execute("ALTER TABLE course_teachers DROP COLUMN teacher_id")
        cursor.execute("ALTER TABLE course_teachers CHANGE COLUMN teacher_id_new teacher_id VARCHAR(64) NOT NULL")
        cursor.execute("ALTER TABLE course_teachers ADD CONSTRAINT fk_ct_teacher FOREIGN KEY (teacher_id) REFERENCES teachers(teacher_id)")

    # For resource_teachers:
    cursor.execute("SHOW COLUMNS FROM resource_teachers LIKE 'teacher_id'")
    if cursor.fetchone():
        cursor.execute("ALTER TABLE resource_teachers DROP COLUMN teacher_id")
        cursor.execute("ALTER TABLE resource_teachers CHANGE COLUMN teacher_id_new teacher_id VARCHAR(64) NOT NULL")
        cursor.execute("ALTER TABLE resource_teachers ADD CONSTRAINT fk_rt_teacher FOREIGN KEY (teacher_id) REFERENCES teachers(teacher_id)")

    # 4. Handle schema changes for students
    cursor.execute("SHOW COLUMNS FROM students LIKE 'id'")
    if cursor.fetchone():
        cursor.execute("ALTER TABLE students MODIFY COLUMN id INT")
        cursor.execute("ALTER TABLE students DROP PRIMARY KEY")
        cursor.execute("ALTER TABLE students DROP COLUMN id")
        cursor.execute("ALTER TABLE students ADD PRIMARY KEY (student_id)")

    # 5. Handle schema changes for deans
    cursor.execute("SHOW COLUMNS FROM deans LIKE 'id'")
    if cursor.fetchone():
        cursor.execute("ALTER TABLE deans MODIFY COLUMN id INT")
        cursor.execute("ALTER TABLE deans DROP PRIMARY KEY")
        cursor.execute("ALTER TABLE deans DROP COLUMN id")
        cursor.execute("ALTER TABLE deans ADD PRIMARY KEY (dean_id)")

    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    print("Database migration completed successfully.")

except Exception as e:
    conn.rollback()
    print(f"Migration failed: {e}")
finally:
    conn.close()
