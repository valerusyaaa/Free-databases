import mysql.connector
import psycopg2
import re

# === 🔹 Конвертер типов MySQL → PostgreSQL ===
def convert_mysql_type_to_pg(mysql_type):
    if not mysql_type:
        return 'TEXT'
    mysql_type = mysql_type.lower().strip()
    mysql_type = re.sub(r'\s+unsigned', '', mysql_type)
    mapping = {
        'tinyint': 'SMALLINT',
        'smallint': 'SMALLINT',
        'mediumint': 'INTEGER',
        'int': 'INTEGER',
        'integer': 'INTEGER',
        'bigint': 'BIGINT',
        'decimal': 'NUMERIC',
        'numeric': 'NUMERIC',
        'float': 'REAL',
        'double': 'DOUBLE PRECISION',
        'bit': 'BIT',
        'char': 'CHAR',
        'varchar': 'VARCHAR',
        'text': 'TEXT',
        'longtext': 'TEXT',
        'mediumtext': 'TEXT',
        'tinytext': 'TEXT',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'time': 'TIME',
        'year': 'INTEGER',
        'json': 'JSON',
    }
    base = mysql_type.split('(')[0]
    pg_type = mapping.get(base, 'TEXT')
    if '(' in mysql_type:
        params = mysql_type.split('(')[1].split(')')[0]
        return f"{pg_type}({params})"
    return pg_type


# === 🔹 Конвертация тела функции ===
def convert_function_body(body):
    if not body:
        return "RETURN NULL;"
    body = re.sub(r'CREATE\s+DEFINER=.*?\s+FUNCTION', 'CREATE FUNCTION', body, flags=re.I)
    body = body.replace('`', '"')
    body = re.sub(r'DELIMITER\s*\$\$', '', body, flags=re.I)
    body = re.sub(r'\$\$\s*DELIMITER\s*;', '', body, flags=re.I)
    body = body.replace('BEGIN', 'BEGIN\n')
    body = body.replace('END', '\nEND')
    body = body.replace('IF ', 'IF ')
    body = re.sub(r'NOW\(\)', 'CURRENT_TIMESTAMP', body)
    body = re.sub(r'CONCAT\((.*?)\)', r'(\1)', body)
    body = re.sub(r'SET\s+(\w+)\s*=\s*(.+?);', r'\1 := \2;', body)
    return body.strip()


# === 🔹 Получение параметров функции ===
def get_function_parameters(mysql_cursor, function_name, db_name):
    query = f"""
        SELECT PARAMETER_NAME, DTD_IDENTIFIER
        FROM information_schema.parameters
        WHERE SPECIFIC_SCHEMA = '{db_name}'
          AND SPECIFIC_NAME = '{function_name}'
          AND ROUTINE_TYPE = 'FUNCTION'
        ORDER BY ORDINAL_POSITION;
    """
    mysql_cursor.execute(query)
    params = []
    for name, dtype in mysql_cursor.fetchall():
        pg_type = convert_mysql_type_to_pg(dtype)
        if name:
            params.append(f"{name} {pg_type}")
    return ', '.join(params)


# === 🔹 Получение тела функции из разных источников ===
def get_function_sql(mysql_cursor, function_name, db_name):
    try:
        # 1️⃣ Основной способ
        mysql_cursor.execute(f"SHOW CREATE FUNCTION `{function_name}`;")
        result = mysql_cursor.fetchone()
        if result and len(result) >= 3:
            print("  ✅ Тело функции получено через SHOW CREATE FUNCTION")
            return result[2]

        # 2️⃣ Через information_schema
        mysql_cursor.execute(f"""
            SELECT ROUTINE_DEFINITION
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = '{db_name}'
              AND ROUTINE_NAME = '{function_name}'
              AND ROUTINE_TYPE = 'FUNCTION';
        """)
        result = mysql_cursor.fetchone()
        if result and result[0]:
            print("  ✅ Тело функции получено через information_schema")
            return result[0]

        # 3️⃣ Через mysql.proc (для старых MySQL)
        mysql_cursor.execute(f"""
            SELECT body FROM mysql.proc
            WHERE db = '{db_name}'
              AND name = '{function_name}'
              AND type = 'FUNCTION';
        """)
        result = mysql_cursor.fetchone()
        if result and result[0]:
            print("  ✅ Тело функции получено через mysql.proc")
            return result[0]

        print("  ❌ Тело функции не найдено")
        return None

    except Exception as e:
        print(f"  ⚠️ Ошибка при получении тела функции {function_name}: {e}")
        return None


# === 🔹 Основная функция миграции ===
def migrate_functions(mysql_db, pg_db):
    print("🚀 Начало миграции функций...")

    # Подключение к MySQL
    try:
        mysql_conn = mysql.connector.connect(
            host="localhost",
            user="migrator",
            password="migrator123",
            database=mysql_db
        )
        mysql_cursor = mysql_conn.cursor()
        print("✅ Подключение к MySQL установлено")
    except Exception as e:
        print(f"❌ Ошибка подключения к MySQL: {e}")
        return

    # Подключение к PostgreSQL
    try:
        pg_conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="893476",
            database=pg_db
        )
        pg_cursor = pg_conn.cursor()
        print("✅ Подключение к PostgreSQL установлено")
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        mysql_conn.close()
        return

    # Получение списка функций
    mysql_cursor.execute(f"""
        SELECT ROUTINE_NAME
        FROM information_schema.routines
        WHERE ROUTINE_SCHEMA = '{mysql_db}'
          AND ROUTINE_TYPE = 'FUNCTION';
    """)
    functions = [row[0] for row in mysql_cursor.fetchall()]
    print(f"📊 Найдено функций: {len(functions)}")
    if functions:
        print(f"📋 Список функций: {functions}")

    migrated = 0
    for function in functions:
        try:
            print(f"🔍 Обрабатываем функцию: {function}")
            params = get_function_parameters(mysql_cursor, function, mysql_db)
            print(f"  📝 Параметры функции: {params if params else '(нет)'}")

            sql_code = get_function_sql(mysql_cursor, function, mysql_db)
            if not sql_code:
                print(f"  ⚠️ Не удалось получить тело функции {function}, создаем заглушку")
                body = f"RAISE NOTICE 'Функция {function} не реализована'; RETURN NULL;"
            else:
                body = convert_function_body(sql_code)
                print(f"  ✅ Тело функции конвертировано")

            # Определяем возвращаемый тип
            return_type = "NUMERIC" if "return" in body.lower() else "VOID"

            create_function_sql = f"""
CREATE OR REPLACE FUNCTION "{function}"({params})
RETURNS {return_type} AS $$
BEGIN
    {body}
END;
$$ LANGUAGE plpgsql;
"""

            pg_cursor.execute(create_function_sql)
            pg_conn.commit()
            print(f"✅ Функция перенесена: {function}")
            migrated += 1

        except Exception as e:
            print(f"❌ Ошибка при миграции функции {function}: {e}")
            pg_conn.rollback()

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()

    print(f"\n🎉 Миграция функций завершена!")
    print(f"📈 Успешно перенесено: {migrated} из {len(functions)} функций")


# === 🔹 Точка входа ===
if __name__ == "__main__":
    migrate_functions(mysql_db="sakila", pg_db="sakila_pg")
