import psycopg2

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE if exists telTb;
            """)
        cur.execute("""
            DROP TABLE if exists clientTb;
            """)
        cur.execute("""
            create table if not exists clientTb (
            id serial primary key,
            name varchar(50) not null,
            fam varchar(50) not null,
            email varchar(50) not null UNIQUE);
             """)
        cur.execute("""
            create table if not exists telTb(
            id_cl Integer not null references clientTb(id) on delete cascade on update cascade,
            tel varchar(20) primary key  not null UNIQUE);
            """)
        conn.commit()
    print("БД cоздана")


def add_client(conn, first_name, last_name, email, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clientTb (name,fam,email) VALUES (%s,%s,%s);
        """, (first_name,last_name,email))
        conn.commit()

def add_client_phone(conn, first_name, last_name, email, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clientTb (name,fam,email) VALUES (%s,%s,%s) RETURNING id;
        """, (first_name,last_name,email))
        conn.commit()
        client_id=cur.fetchone()

        cur.execute("""
                    INSERT INTO telTb (id_cl, tel) VALUES (%s,%s);
                """, (client_id, phone))
        conn.commit()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO telTb (id_cl, tel) VALUES (%s,%s);
        """, (client_id,phone))
        conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, email=None):
        # pass
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT clienttb.name, clienttb.fam, clienttb.email,clienttb.id FROM clienttb
                WHERE clienttb.id = %s; 
                """,
                (client_id,)  # Передаем id клиента
            )
            sel_rez = cur.fetchone()  # Получаем данные пользователя в виде кортежа из запроса и сохраняем в переменную
            if not sel_rez:  # Если селект данные не вернул, то проваливаемся в блок
                return "Такого клиента не существует"  # Сообщаем, что такого пользователя нет
            if first_name is None:  # Если имя пустое, то проваливаемся в блок
                first_name = sel_rez[0]  # Изменяем имя клиента на значение из кортежа, который вернулся из запроса
            if last_name is None:  # Если фамилия пустая, то проваливаемся в блок
                last_name = sel_rez[1]  # Изменяем фамилию клиента на значение из кортежа, который вернулся из запроса
            if email is None:  # Если почта пустая, то проваливаемся в блок
                email = sel_rez[2]  # Изменяем почту клиента на значение из кортежа, который вернулся из запроса
            cur.execute(
                """
                UPDATE clienttb 
                SET name = %s, fam = %s, email = %s 
                WHERE id = %s; 
                """,
                (first_name, last_name, email, client_id)
            )
            conn.commit()
        return "Пользователь успешно изменен"

def delete_phone(conn, client_id, phone=None):
    with conn.cursor() as cur:
        del_str="""
            DELETE FROM teltb WHERE telTb.id_cl = %s AND teltb.tel = %s RETURNING id_cl, tel;
            """
        cur.execute(del_str, (client_id,phone))
        if not cur.fetchone():  # Проверяем не пустая ли коллекция вернулась
            return "Указанные для удаления данные не найдены"  # Возвращаем сообщение что такого номера нет
        conn.commit()
        return "Удаление телефона ", phone, "произведено успешно"  # Сообщаем об успешной операции

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        del_str1 = """
            DELETE FROM clienttb WHERE clienttb.id = %s RETURNING id;
            """
        cur.execute(del_str1, (client_id,))
        if not cur.fetchone():  # Проверяем не пустая ли коллекция вернулась
            return "Указанные для удаления данные не найдены"  # Возвращаем сообщение что такого номера нет
        conn.commit()
        return "Удаление клиента с ID=",str(client_id), "произведено успешно"  # Сообщаем об успешной операции

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    print("Поиск для: ", first_name, last_name, email, phone)

    with conn.cursor() as curs:
        if first_name is None:  # Если имя не было передано
            first_name = '%'  # Определяем новое значение, которое означает, что здесь может быть любая строка
        if last_name is None:  # Если фамилия не была передана
            last_name = '%'  # Определяем новое значение, которое означает, что здесь может быть любая строка
        if email is None:  # Если почта не была передана
            email = '%'  # Определяем новое значение, которое означает, что здесь может быть любая строка
        param1 = [first_name, last_name, email]  # Создаем список из имени, фамилии и почты
        new_str = ''  # Определяем переменную с пустой строкой. Далее эта строка будет вставляться в тело запоса.
        if phone:  # Если телефон содержит значение
            new_str = ' AND teltb.tel= %s::text'  # Присваиваем переменной, которую определили через строку выше,
            # новое значение с условием поиска телефона.Вместо первых точек указываем столбец с номерами из таблицы номеров.
        # else:
        #     phone='%'
            param1.append(phone)  # Добавляем в ранее созданный список телефон, который передали в функцию.
        select_str = f""" 
                SELECT  
                    clientTb.name, clientTb.fam, clientTb.email, 
                    CASE
                        WHEN ARRAY_AGG(teltb.tel) = '{{Null}}' THEN ARRAY[]::TEXT[] 
                        ELSE ARRAY_AGG(teltb.tel) 
                    END phones 
                FROM clientTb 
                LEFT JOIN telTb ON clientTb.id = telTb.id_cl 
                WHERE clientTb.name ILIKE %s AND clientTb.fam ILIKE %s AND clientTb.email ILIKE %s{new_str} 
                GROUP BY clientTb.name,clientTb.fam, clientTb.email;
                """
        curs.execute(select_str, param1)  # Передаем список или кортеж с значениями
        qw = curs.fetchall()
        print(qw)

    return qw


with psycopg2.connect(database="client_db", user="postgres", password="qwer3") as connec:
    create_db(connec)
    add_client(connec, "Сергей", "Иванов", "090090002@qas.ru")
    add_client(connec, "Борис", "Борисов", "020020002@qas.ru")
    add_client(connec, "Михаил", "Михайлов", "030030003@qas.ru")
    add_client(connec, "Андрей", "Андреев", "040040004@qas.ru")
    add_client(connec, "Николай", "Николаев", "050050005@qas.ru")
    add_client(connec, "Петр", "Петров", "060060006@qas.ru")
    add_client(connec, "Николай", "Петров", "010010001@qas.ru")
    add_phone(connec, 1, "010010001")
    add_phone(connec, 1, "010010002")
    add_phone(connec, 2, "020020002")
    add_phone(connec, 3, "030030003")
    add_phone(connec, 4, "040040004")
    add_phone(connec, 4, "040040002")
    add_phone(connec, 5, "050050005")
    add_phone(connec, 7, "070070001")
    add_phone(connec, 5, "070070002")
    add_client_phone(connec,"Анна", "Катаева","070070007@qas.ru","070070007")

    find_client(connec, last_name='Петров')
    print(delete_phone(connec,client_id=7,phone='070070002'))
    print(delete_client(connec, client_id=12))
    print(change_client(connec, client_id=7, last_name='Петров'))
