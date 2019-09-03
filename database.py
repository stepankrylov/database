import psycopg2

"""Создание таблиц"""
def create_db(pas):
    with psycopg2.connect(dbname="test_db", user="postgres", password=pas, host="127.0.0.1", port="5432") as conn:
        with conn.cursor() as cur:

            cur.execute("""CREATE TABLE Course(
                id serial PRIMARY KEY,
                name varchar(100) NOT NULL);
            """)

            cur.execute("""CREATE TABLE Student(
                id serial PRIMARY KEY,
                name varchar(100) NOT NULL,
                gpa numeric(10, 2),
                birth timestamp with time zone);
            """)

            cur.execute("""CREATE TABLE Distribution(
                id serial PRIMARY KEY,
                name_student integer REFERENCES Student(id),
                name_course integer REFERENCES Course(id));
            """)

"""Запись студента"""
def add_student(pas, name, gpa, birth):
    with psycopg2.connect(dbname="test_db", user="postgres", password=pas, host="127.0.0.1", port="5432") as conn:
        with conn.cursor() as cur:

            cur.execute("""INSERT INTO Student (name, gpa, birth) VALUES (%s, %s, %s);
            """, (name, gpa, birth))

"""Запись нового курса"""
def add_course(pas, name_course):
    with psycopg2.connect(dbname="test_db", user="postgres", password=pas, host="127.0.0.1", port="5432") as conn:
        with conn.cursor() as cur:

            cur.execute("""INSERT INTO Course (name) VALUES (%s);
            """,(name_course,))

"""Распределение студента на курс"""
def add_distribution(pas, id_student, id_course):
    with psycopg2.connect(dbname="test_db", user="postgres", password=pas, host="127.0.0.1", port="5432") as conn:
        with conn.cursor() as cur:

            cur.execute("""INSERT INTO Distribution (name_student, name_course) VALUES (%s, %s);
            """, (id_student, id_course))

"""Список студентов и их курсов"""
def get_students(pas):
    with psycopg2.connect(dbname="test_db", user="postgres", password=pas, host="127.0.0.1", port="5432") as conn:
        with conn.cursor() as cur:

            cur.execute("""
                            SELECT Student.name, Course.name from Distribution 
                            join Student on Distribution.name_student = Student.id 
                            join Course on Distribution.name_course = Course.id;
            """)
            print(cur.fetchall())

"""Запись студента и распределение его на курс"""
def add_student_distribution(pas, name, gpa, birth, name_course):
    with psycopg2.connect(dbname="test_db", user="postgres", password=pas, host="127.0.0.1", port="5432") as conn:
        with conn.cursor() as cur:

            cur.execute("""INSERT INTO Student (name, gpa, birth) VALUES (%s, %s, %s);
            """, (name, gpa, birth))

            cur.execute("""SELECT id from Student WHERE name = %s;
            """, (name,))

            student_id = cur.fetchall()[0][0]

            cur.execute("""INSERT INTO Distribution (name_student, name_course) VALUES (%s, %s);
            """, (student_id, name_course))

"""Список студентов определенного курса"""
def get_students_course(pas, name_course):
    with psycopg2.connect(dbname="test_db", user="postgres", password=pas, host="127.0.0.1", port="5432") as conn:
        with conn.cursor() as cur:

            cur.execute("""SELECT Student.name from Distribution 
                            join Student on Distribution.name_student = Student.id 
                            join Course on Distribution.name_course = Course.id WHERE Course.id = %s;
            """, (name_course,))
            print(cur.fetchall())

"""Данные о студенте"""
def get_student(pas, student_id):
    with psycopg2.connect(dbname="test_db", user="postgres", password=pas, host="127.0.0.1", port="5432") as conn:
        with conn.cursor() as cur:

            cur.execute("""SELECT Student.name, Student.gpa, Student.birth, Course.name from Distribution 
                            join Student on Distribution.name_student = Student.id 
                            join Course on Distribution.name_course = Course.id WHERE Student.id = %s;
            """, (student_id,))
            print(cur.fetchall())

if __name__ == "__main__":
    get_student('qwerty', 2)