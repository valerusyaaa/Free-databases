import mysql.connector
import psycopg2
import re

def convert_mysql_type_to_pg(mysql_type):
    """
    Конвертирует типы данных MySQL в PostgreSQL.
    """
    if mysql_type is None:
        return 'TEXT'
    
    mysql_type = mysql_type.lower().strip()
    
    # Убираем UNSIGNED и другие модификаторы
    mysql_type = re.sub(r'\s+unsigned', '', mysql_type, flags=re.IGNORECASE)
    mysql_type = re.sub(r'\s+zerofill', '', mysql_type, flags=re.IGNORECASE)
    
    # Базовое сопоставление типов
    type_mapping = {
        'tinyint': 'SMALLINT',
        'smallint': 'SMALLINT',
        'mediumint': 'INTEGER',
        'int': 'INTEGER',
        'integer': 'INTEGER',
        'bigint': 'BIGINT',
        'decimal': 'DECIMAL',
        'numeric': 'NUMERIC',
        'float': 'REAL',
        'double': 'DOUBLE PRECISION',
        'real': 'REAL',
        'bit': 'BIT',
        'char': 'CHAR',
        'varchar': 'VARCHAR',
        'binary': 'BYTEA',
        'varbinary': 'BYTEA',
        'tinyblob': 'BYTEA',
        'blob': 'BYTEA',
        'mediumblob': 'BYTEA',
        'longblob': 'BYTEA',
        'tinytext': 'TEXT',
        'text': 'TEXT',
        'mediumtext': 'TEXT',
        'longtext': 'TEXT',
        'enum': 'TEXT',
        'set': 'TEXT',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'time': 'TIME',
        'year': 'INTEGER',
        'json': 'JSON'
    }
    
    # Извлекаем базовый тип и параметры
    base_type = mysql_type.split('(')[0]
    pg_base_type = type_mapping.get(base_type, 'TEXT')
    
    # Сохраняем параметры типа (размер, точность)
    if '(' in mysql_type:
        params = mysql_type.split('(')[1].split(')')[0]
        return f"{pg_base_type}({params})"
    else:
        return pg_base_type

def convert_procedure_body(body):
    """
    Конвертирует тело процедуры MySQL в PL/pgSQL для PostgreSQL.
    """
    if body is None:
        return ""
    
    # Убираем DEFINER и обратные кавычки
    body = re.sub(r'CREATE\s+DEFINER=`[^`]+`@`[^`]+`\s+PROCEDURE', 'CREATE PROCEDURE', body, flags=re.IGNORECASE)
    body = body.replace('`', '"')
    
    # Конвертируем DELIMITER (убираем)
    body = re.sub(r'DELIMITER\s*\$\$', '', body, flags=re.IGNORECASE)
    body = re.sub(r'\$\$\s*DELIMITER\s*;', '', body, flags=re.IGNORECASE)
    
    # Конвертируем SET variable = value -> variable := value;
    body = re.sub(r'SET\s+(@?)(\w+)\s*=\s*(.+?);', r'\2 := \3;', body, flags=re.IGNORECASE)
    
    # Конвертируем DECLARE переменные
    def convert_declare(match):
        var_name = match.group(1)
        data_type = match.group(2)
        pg_type = convert_mysql_type_to_pg(data_type)
        return f"{var_name} {pg_type};"
    
    body = re.sub(r'DECLARE\s+(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)', convert_declare, body, flags=re.IGNORECASE)
    
    # Конвертируем IF конструкции
    body = re.sub(r'IF\s+(.+?)\s+THEN\s*', r'IF \1 THEN\n', body, flags=re.IGNORECASE)
    body = re.sub(r'ELSEIF\s+(.+?)\s+THEN\s*', r'ELSIF \1 THEN\n', body, flags=re.IGNORECASE)
    body = re.sub(r'END\s+IF;', r'END IF;', body, flags=re.IGNORECASE)
    
    # Конвертируем LOOP конструкции
    body = re.sub(r'LOOP\s*', r'LOOP\n', body, flags=re.IGNORECASE)
    body = re.sub(r'END\s+LOOP;', r'END LOOP;', body, flags=re.IGNORECASE)
    
    # Конвертируем WHILE конструкции
    body = re.sub(r'WHILE\s+(.+?)\s+DO\s*', r'WHILE \1 LOOP\n', body, flags=re.IGNORECASE)
    body = re.sub(r'END\s+WHILE;', r'END LOOP;', body, flags=re.IGNORECASE)
    
    # Конвертируем REPEAT конструкции
    body = re.sub(r'REPEAT\s*', r'LOOP\n', body, flags=re.IGNORECASE)
    body = re.sub(r'UNTIL\s+(.+?)\s+END\s+REPEAT;', r'EXIT WHEN \1;\nEND LOOP;', body, flags=re.IGNORECASE)
    
    # Конвертируем NOW() -> CURRENT_TIMESTAMP
    body = body.replace('NOW()', 'CURRENT_TIMESTAMP')
    
    # Конвертируем CONCAT -> ||
    body = re.sub(r'CONCAT\((.*?)\)', r'(\1)', body)
    
    # Убираем специфичные для MySQL функции или заменяем их
    body = body.replace('CURDATE()', 'CURRENT_DATE')
    body = body.replace('CURTIME()', 'CURRENT_TIME')
    
    # Конвертируем SELECT ... INTO
    body = re.sub(r'SELECT\s+(.+?)\s+INTO\s+(\w+)', r'SELECT \1 INTO \2', body, flags=re.IGNORECASE)
    
    return body.strip()

def get_procedure_parameters(mysql_cursor, procedure_name, db_name):
    """
    Получает параметры процедуры из MySQL.
    """
    try:
        mysql_cursor.execute(f"""
            SELECT PARAMETER_MODE, PARAMETER_NAME, DTD_IDENTIFIER
            FROM information_schema.parameters 
            WHERE SPECIFIC_SCHEMA = '{db_name}' 
            AND SPECIFIC_NAME = '{procedure_name}'
            AND ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ORDINAL_POSITION
        """)
        
        in_params = []
        out_params = []
        
        for row in mysql_cursor.fetchall():
            mode, name, data_type = row
            pg_type = convert_mysql_type_to_pg(data_type)
            
            if name:
                if mode == 'IN':
                    in_params.append(f"{name} {pg_type}")
                elif mode == 'OUT':
                    out_params.append(f"{name} {pg_type}")
                elif mode == 'INOUT':
                    # В PostgreSQL INOUT параметры не поддерживаются в функциях, 
                    # используем IN + OUT отдельно
                    in_params.append(f"{name}_in {pg_type}")
                    out_params.append(f"{name}_out {pg_type}")
        
        # Формируем параметры для PostgreSQL функции
        all_params = in_params + [f"OUT {param}" for param in out_params]
        return ', '.join(all_params) if all_params else ''
        
    except Exception as e:
        print(f"⚠️ Ошибка при получении параметров процедуры {procedure_name}: {e}")
        return ''

def get_procedure_return_type(out_params_count):
    """
    Определяет тип возвращаемого значения в зависимости от OUT параметров.
    """
    if out_params_count == 0:
        return 'VOID'
    elif out_params_count == 1:
        return 'INTEGER'  # Базовый тип, может потребоваться уточнение
    else:
        # Для нескольких OUT параметров используем TABLE
        return 'TABLE(result INTEGER)'

def get_procedure_sql(mysql_cursor, procedure_name, db_name):
    """
    Получает SQL определение процедуры из MySQL.
    """
    try:
        # Способ 1: SHOW CREATE PROCEDURE
        print(f"  🔎 Пытаемся получить SQL через SHOW CREATE PROCEDURE...")
        mysql_cursor.execute(f"SHOW CREATE PROCEDURE `{procedure_name}`")
        result = mysql_cursor.fetchone()
        
        if result and len(result) >= 3:
            print(f"  ✅ SQL получен через SHOW CREATE PROCEDURE")
            return result[2]  # Третье поле содержит SQL
        
        # Способ 2: information_schema.ROUTINES
        print(f"  🔎 Пытаемся получить SQL через information_schema...")
        mysql_cursor.execute(f"""
            SELECT ROUTINE_DEFINITION 
            FROM information_schema.ROUTINES 
            WHERE ROUTINE_SCHEMA = '{db_name}' 
            AND ROUTINE_NAME = '{procedure_name}' 
            AND ROUTINE_TYPE = 'PROCEDURE'
        """)
        result = mysql_cursor.fetchone()
        if result and result[0]:
            print(f"  ✅ SQL получен через information_schema")
            return result[0]
            
        print(f"  ❌ Все способы получения SQL не удались")
        return None
            
    except Exception as e:
        print(f"  ❌ Ошибка при получении SQL процедуры {procedure_name}: {e}")
        return None

def count_out_parameters(mysql_cursor, procedure_name, db_name):
    """
    Подсчитывает количество OUT параметров процедуры.
    """
    try:
        mysql_cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.parameters 
            WHERE SPECIFIC_SCHEMA = '{db_name}' 
            AND SPECIFIC_NAME = '{procedure_name}'
            AND ROUTINE_TYPE = 'PROCEDURE'
            AND PARAMETER_MODE IN ('OUT', 'INOUT')
        """)
        return mysql_cursor.fetchone()[0]
    except:
        return 0

def create_procedure_from_template(procedure_name, parameters, out_params_count):
    """
    Создает шаблон процедуры для случаев, когда не удается получить исходный код.
    """
    print(f"  🔧 Создаем шаблон процедуры {procedure_name}")
    
    if out_params_count > 0:
        # Для процедур с OUT параметрами
        template = f"""
    -- Исходный код процедуры {procedure_name} не был получен из MySQL
    -- Необходимо реализовать логику вручную
    
    -- Пример для OUT параметров:
    -- p_film_count := 0; -- Замените на реальную логику
    RAISE NOTICE 'Процедура {procedure_name} вызвана, но не реализована';
    """
    else:
        # Для процедур без OUT параметров
        template = f"""
    RAISE NOTICE 'Процедура {procedure_name} вызвана, но не реализована';
    -- Исходный код процедуры не был получен из MySQL
    -- Необходимо реализовать логику вручную
    """
    
    return template

def migrate_procedures(mysql_db, pg_db):
    print("🚀 Начало миграции процедур...")

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
            database=pg_db,
            user="postgres",
            password="893476"
        )
        pg_cursor = pg_conn.cursor()
        print("✅ Подключение к PostgreSQL установлено")
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        mysql_cursor.close()
        mysql_conn.close()
        return

    # Получаем список процедур
    try:
        mysql_cursor.execute(f"""
            SELECT ROUTINE_NAME
            FROM information_schema.routines 
            WHERE ROUTINE_SCHEMA = '{mysql_db}' 
            AND ROUTINE_TYPE = 'PROCEDURE'
        """)
        procedures = [row[0] for row in mysql_cursor.fetchall()]
        print(f"📊 Найдено процедур: {len(procedures)}")
        
        if procedures:
            print(f"📋 Список процедур: {procedures}")
    except Exception as e:
        print(f"❌ Ошибка при получении списка процедур: {e}")
        procedures = []

    migrated_count = 0
    for procedure in procedures:
        try:
            print(f"🔍 Обрабатываем процедуру: {procedure}")

            # Получаем параметры процедуры
            parameters = get_procedure_parameters(mysql_cursor, procedure, mysql_db)
            print(f"  📝 Параметры процедуры: {parameters}")

            # Подсчитываем OUT параметры
            out_params_count = count_out_parameters(mysql_cursor, procedure, mysql_db)
            print(f"  📊 Количество OUT параметров: {out_params_count}")

            # Получаем полное определение процедуры
            procedure_sql = get_procedure_sql(mysql_cursor, procedure, mysql_db)
            
            body_pg = ""
            if procedure_sql:
                # Конвертируем тело процедуры
                body_pg = convert_procedure_body(procedure_sql)
                print(f"  ✅ Исходный код получен и конвертирован")
            else:
                # Создаем шаблон процедуры
                body_pg = create_procedure_from_template(procedure, parameters, out_params_count)
                print(f"  ⚠️ Используем шаблон для процедуры {procedure}")

            # Определяем возвращаемый тип
            return_type = 'VOID' if out_params_count == 0 else 'INTEGER'
            
            # Создаём функцию в PostgreSQL
            create_function = f"""
CREATE OR REPLACE FUNCTION "{procedure}"({parameters})
RETURNS {return_type} AS $$
BEGIN
    {body_pg}
END;
$$ LANGUAGE plpgsql;
"""
            
            print(f"  🛠️ Создаем процедуру в PostgreSQL...")
            pg_cursor.execute(create_function)
            pg_conn.commit()
            print(f"✅ Процедура перенесена: {procedure}")
            migrated_count += 1

        except Exception as e:
            print(f"❌ Ошибка при миграции процедуры {procedure}: {e}")
            import traceback
            traceback.print_exc()
            pg_conn.rollback()

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()
    
    print(f"\n🎉 Миграция процедур завершена!")
    print(f"📈 Успешно перенесено: {migrated_count} из {len(procedures)} процедур")

if __name__ == "__main__":
    migrate_procedures(mysql_db="sakila", pg_db="sakila_pg")