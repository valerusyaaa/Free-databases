import mysql.connector
import psycopg2

# Сопоставление типов MySQL → PostgreSQL
def mysql_to_postgres_type(mysql_type):
    mysql_type = mysql_type.lower()
    if "int" in mysql_type:
        return "INTEGER"
    elif "bigint" in mysql_type:
        return "BIGINT"
    elif "varchar" in mysql_type or "text" in mysql_type or "char" in mysql_type:
        return "TEXT"
    elif "datetime" in mysql_type or "timestamp" in mysql_type:
        return "TIMESTAMP"
    elif "decimal" in mysql_type or "numeric" in mysql_type:
        return "NUMERIC"
    elif "float" in mysql_type or "double" in mysql_type:
        return "REAL"
    else:
        return "TEXT"  # default fallback

def migrate_all_tables():
    print("🚀 Начало миграции...")

    # Подключение к MySQL
    mysql_conn = mysql.connector.connect(
        host="localhost",
        user="migrator",
        password="migrator123",
        database="sakila"
    )
    mysql_cursor = mysql_conn.cursor()

    # Подключение к PostgreSQL
    pg_conn = psycopg2.connect(
        host="localhost",
        database="sakila_pg",
        user="postgres",
        password="893476"
    )
    pg_cursor = pg_conn.cursor()

    # Получаем список таблиц (исключаем ML таблицы)
    mysql_cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'sakila'
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE 'model%'
        AND table_name NOT LIKE '%cluster%'
        AND table_name NOT IN ('scripts', 'models')
    """)
    tables = [row[0] for row in mysql_cursor.fetchall()]

    print(f"📊 Найдено таблиц для миграции: {len(tables)}")
    print("Таблицы:", tables)

    for table in tables:
        try:
            print(f"\n🔄 Мигрируем таблицу: {table}")

            # Получаем описание колонок из MySQL
            mysql_cursor.execute(f"DESCRIBE {table}")
            columns_info = mysql_cursor.fetchall()

            columns_def = []
            columns_names = []
            for col in columns_info:
                name = col[0]
                type_mysql = col[1]
                type_pg = mysql_to_postgres_type(type_mysql)
                columns_def.append(f"{name} {type_pg}")
                columns_names.append(name)

            # Создаём таблицу в PostgreSQL, если её нет
            create_sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns_def)})"
            pg_cursor.execute(create_sql)
            pg_conn.commit()

            # Очищаем таблицу перед вставкой
            pg_cursor.execute(f"TRUNCATE TABLE {table}")
            pg_conn.commit()

            # Мигрируем данные пачками
            placeholders = ', '.join(['%s'] * len(columns_names))
            batch_size = 1000
            total_rows = 0

            mysql_cursor.execute(f"SELECT * FROM {table}")
            while True:
                rows = mysql_cursor.fetchmany(batch_size)
                if not rows:
                    break
                pg_cursor.executemany(
                    f"INSERT INTO {table} VALUES ({placeholders})",
                    rows
                )
                total_rows += len(rows)
                print(f"   📦 Передано {total_rows} строк...", end='\r')

            pg_conn.commit()
            print(f"✅ Успешно: {table} - {total_rows} строк")

        except Exception as e:
            print(f"❌ Ошибка в таблице {table}: {e}")
            pg_conn.rollback()

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()
    print("\n🎉 Миграция завершена!")

if __name__ == "__main__":
    migrate_all_tables()
