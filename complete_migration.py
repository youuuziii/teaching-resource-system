import os
from dotenv import load_dotenv
import pymysql

load_dotenv()

# Database connection setup
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
db_name = db_params.split('?')[0]

conn = pymysql.connect(
    host=host, 
    port=port, 
    user=user, 
    password=password, 
    database=db_name
)
cursor = conn.cursor()

def drop_foreign_key(table, constraint):
    try:
        cursor.execute(f"ALTER TABLE {table} DROP FOREIGN KEY {constraint}")
        print(f"Dropped FK {constraint} from {table}")
    except Exception as e:
        print(f"Note: Could not drop FK {constraint} from {table}: {e}")

def drop_column(table, col):
    try:
        cursor.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
        print(f"Dropped column {col} from {table}")
    except Exception as e:
        print(f"Note: Could not drop column {col} from {table}: {e}")

try:
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

    # 1. Delete the first two records in teachers
    # We should also delete their associated records in course_teachers and resource_teachers
    cursor.execute("SELECT teacher_id FROM teachers ORDER BY teacher_id ASC LIMIT 2")
    to_delete = [r[0] for r in cursor.fetchall()]
    for tid in to_delete:
        # Delete from child tables using teacher_id_new since teacher_id might be gone or inconsistent
        cursor.execute("DELETE FROM course_teachers WHERE teacher_id_new = %s", (tid,))
        cursor.execute("DELETE FROM resource_teachers WHERE teacher_id_new = %s", (tid,))
        cursor.execute("DELETE FROM teachers WHERE teacher_id = %s", (tid,))
        print(f"Deleted teacher {tid} and related records")

    # 2. Clean up orphaned records (where teacher_id_new is NULL or doesn't exist in teachers)
    cursor.execute("DELETE FROM course_teachers WHERE teacher_id_new IS NULL OR teacher_id_new NOT IN (SELECT teacher_id FROM teachers)")
    cursor.execute("DELETE FROM resource_teachers WHERE teacher_id_new IS NULL OR teacher_id_new NOT IN (SELECT teacher_id FROM teachers)")
    print("Cleaned up orphaned records in child tables")

    # 3. Reformat teacher_id in teachers table
    cursor.execute("SELECT teacher_id FROM teachers ORDER BY teacher_id ASC")
    teachers = [r[0] for r in cursor.fetchall()]
    
    # Use temporary IDs to avoid collisions
    for i, tid in enumerate(teachers):
        temp_id = f"TEMP_{i}"
        cursor.execute("UPDATE teachers SET teacher_id = %s WHERE teacher_id = %s", (temp_id, tid))
        cursor.execute("UPDATE course_teachers SET teacher_id_new = %s WHERE teacher_id_new = %s", (temp_id, tid))
        cursor.execute("UPDATE resource_teachers SET teacher_id_new = %s WHERE teacher_id_new = %s", (temp_id, tid))

    # Now set the final t01, t02 format
    for i, _ in enumerate(teachers):
        new_id = f"t{i+1:02d}"
        temp_id = f"TEMP_{i}"
        cursor.execute("UPDATE teachers SET teacher_id = %s WHERE teacher_id = %s", (new_id, temp_id))
        cursor.execute("UPDATE course_teachers SET teacher_id_new = %s WHERE teacher_id_new = %s", (new_id, temp_id))
        cursor.execute("UPDATE resource_teachers SET teacher_id_new = %s WHERE teacher_id_new = %s", (new_id, temp_id))
    print(f"Reformatted {len(teachers)} teachers to t01, t02...")

    # 4. Finalize course_teachers
    drop_column("course_teachers", "teacher_id")
    cursor.execute("ALTER TABLE course_teachers CHANGE COLUMN teacher_id_new teacher_id VARCHAR(64) NOT NULL")
    cursor.execute("ALTER TABLE course_teachers ADD CONSTRAINT fk_ct_teacher_id FOREIGN KEY (teacher_id) REFERENCES teachers(teacher_id)")

    # 5. Finalize resource_teachers
    # Drop existing unique constraint if it exists
    try:
        cursor.execute("ALTER TABLE resource_teachers DROP INDEX uq_resource_teachers_resource_id")
    except:
        pass
    drop_column("resource_teachers", "teacher_id")
    cursor.execute("ALTER TABLE resource_teachers CHANGE COLUMN teacher_id_new teacher_id VARCHAR(64) NOT NULL")
    cursor.execute("ALTER TABLE resource_teachers ADD UNIQUE KEY uq_rt_res_teacher (resource_id, teacher_id)")
    cursor.execute("ALTER TABLE resource_teachers ADD CONSTRAINT fk_rt_teacher_id FOREIGN KEY (teacher_id) REFERENCES teachers(teacher_id)")

    # 6. Remove employee_id from teachers
    drop_column("teachers", "employee_id")

    # 7. Handle Students table
    cursor.execute("SHOW COLUMNS FROM students LIKE 'id'")
    if cursor.fetchone():
        cursor.execute("ALTER TABLE students MODIFY COLUMN id INT")
        cursor.execute("ALTER TABLE students DROP PRIMARY KEY")
        cursor.execute("ALTER TABLE students DROP COLUMN id")
        cursor.execute("ALTER TABLE students ADD PRIMARY KEY (student_id)")
        print("Updated students table")

    # 8. Handle Deans table
    cursor.execute("SHOW COLUMNS FROM deans LIKE 'id'")
    if cursor.fetchone():
        cursor.execute("ALTER TABLE deans MODIFY COLUMN id INT")
        cursor.execute("ALTER TABLE deans DROP PRIMARY KEY")
        cursor.execute("ALTER TABLE deans DROP COLUMN id")
        cursor.execute("ALTER TABLE deans ADD PRIMARY KEY (dean_id)")
        print("Updated deans table")

    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    print("Migration successful!")

except Exception as e:
    conn.rollback()
    print(f"Migration failed: {e}")
    import traceback
    traceback.print_exc()
finally:
    conn.close()
