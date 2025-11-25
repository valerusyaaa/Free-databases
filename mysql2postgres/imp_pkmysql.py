import mysql.connector
import psycopg2

# === НАСТРОЙКИ ПОДКЛЮЧЕНИЙ ===
mysql_config = {
    'host':"localhost",
    'user':"migrator",
    'password':"migrator123",
    'database':'sakila'
}

pg_config = {
    'host': 'localhost',
    'user': 'postgres',
    'password': '',
    'dbname': 'sakila_pg'
}

# === ПОДКЛЮЧЕНИЕ ===
mysql_conn = mysql.connector.connect(**mysql_config)
pg_conn = psycopg2.connect(**pg_config)
mysql_cur = mysql_conn.cursor(dictionary=True)
pg_cur = pg_conn.cursor()

print("🚀 Начало миграции PRIMARY KEY...")

# === ПОЛУЧАЕМ СПИСОК ВСЕХ PRIMARY KEY ===
mysql_cur.execute("""
    SELECT 
        TABLE_NAME,
        GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) AS columns_list
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE CONSTRAINT_NAME = 'PRIMARY'
      AND TABLE_SCHEMA = DATABASE()
    GROUP BY TABLE_NAME;
""")
pks = mysql_cur.fetchall()

print(f"📊 Найдено первичных ключей: {len(pks)}")

success = 0

for pk in pks:
    table = pk["TABLE_NAME"]
    columns = pk["columns_list"].split(",")

    pk_name = f"pk_{table}"
    cols = ", ".join([f'"{c.strip()}"' for c in columns])
    sql = f'ALTER TABLE "{table}" ADD CONSTRAINT "{pk_name}" PRIMARY KEY ({cols});'

    try:
        pg_cur.execute(sql)
        pg_conn.commit()
        print(f"✅ Добавлен PK для таблицы {table}: ({cols})")
        success += 1
    except psycopg2.Error as e:
        pg_conn.rollback()
        print(f"⚠️ Ошибка при добавлении PK для {table}: {e.pgerror.strip()}")

print(f"\n🎉 Миграция PRIMARY KEY завершена!")
print(f"📈 Успешно добавлено: {success} из {len(pks)} первичных ключей.")

# === ЗАКРЫВАЕМ СОЕДИНЕНИЯ ===
mysql_cur.close()
mysql_conn.close()
pg_cur.close()
pg_conn.close()
