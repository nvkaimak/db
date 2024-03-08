import psycopg2
from psycopg2.sql import SQL, Identifier
from conn import *


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users(
                id SERIAL PRIMARY KEY
                ,first_name VARCHAR(40) NOT NULL
                ,last_name VARCHAR(40) NOT NULL
                ,email VARCHAR(40) NOT NULL);
            """)
        conn.commit()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS phone_users(
                id INT REFERENCES users(id)
                ,phone VARCHAR(40) NOT NULL);
                        """)
        conn.commit()

def insert_data(conn, first_name, last_name, email, phone = None):
        arg_list = {'first_name': first_name, "last_name": last_name, 'email': email}
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO users(first_name, last_name, email)
                SELECT  %(first_name)s, %(last_name)s, %(email)s
                WHERE NOT EXISTS(
                 SELECT 1
                 FROM users
                 WHERE first_name= %(first_name)s
                 AND last_name = %(last_name)s
                AND email = %(email)s)
                RETURNING id;""", arg_list)
            conn.commit()

            id = cur.fetchone()
            if id:
                if phone:
                    with conn.cursor() as cur:
                        cur.execute("""
                           INSERT INTO phone_users
                           VALUES (%s,%s);"""
                            , (id[0], phone))

                        conn.commit()
            else :
                print("Такой пользователь есть.")

def find_users(conn,first_name=None, last_name=None,email = None, phone = None):
    arg_list = {'first_name': first_name, "last_name": last_name, 'email': email, 'phone' : phone}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM users
            LEFT JOIN phone_users USING(id)
            WHERE (first_name = %(first_name)s OR %(first_name)s IS NULL)
            AND (last_name = %(last_name)s OR %(last_name)s IS NULL)
            AND (email = %(email)s OR %(email)s IS NULL)
            AND (phone = %(phone)s OR %(phone)s IS NULL);
            """, arg_list)
        print(cur.fetchone())

def add_phones(conn, id, phone):
    arg_list = {'id': int(id), 'phone': str(phone)}
    with conn.cursor() as cur:
        cur.execute("""
           INSERT INTO phone_users
           SELECT %(id)s, %(phone)s
           WHERE NOT EXISTS(
                 SELECT 1
                 FROM phone_users
                 WHERE id = %(id)s
                 AND phone = %(phone)s)
            RETURNING id;""", arg_list)

        conn.commit()
        if not cur.fetchone():
            print('Такой номер у пользователя уже существует')

def update_data(conn, id, first_name=None, last_name=None, email=None):
    arg_list = {'first_name': first_name, "last_name": last_name, 'email': email}
    for key, arg in arg_list.items():
        if arg:
            with conn.cursor() as cur:
                cur.execute(SQL("""UPDATE users 
                SET {}=%s WHERE id=%s""").format(Identifier(key)), (arg, id))
                conn.commit()

def delete_phone(conn, phone):
    with conn.cursor() as cur:
        cur.execute("""
           DELETE FROM phone_users 
           WHERE phone = %s;"""
            , (phone,))
        conn.commit()
def delete_user(conn, id):
    with conn.cursor() as cur:
        cur.execute("""
                   DELETE FROM phone_users 
                   WHERE id = %s;"""
                    , (id,))
        conn.commit()

        cur.execute("""
           DELETE FROM users 
           WHERE id = %s;"""
            , (id,))
        conn.commit()




with psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD) as conn:
    create_db(conn)
    insert_data(conn,"Sasha", "kay", "aaa@mail.ru", phone=447789)
    insert_data(conn,"Nata", "Kay", "ooo@mail.ru")
    add_phones(conn,1,  8543)
    add_phones(conn, 1, 8543)
    find_users(conn,email="ooo@mail.ru")
    delete_user(conn,1)