import mysql.connector
import psycopg2
import re

def extract_trigger_body(body, trigger_name, event_manipulation):
    """
    Извлекает тело триггера MySQL и конвертирует его в PL/pgSQL.
    """
    # Убираем DEFINER и обратные кавычки
    body = re.sub(r'CREATE DEFINER=`.*?`@`.*?` TRIGGER', 'CREATE TRIGGER', body, flags=re.IGNORECASE)
    body = body.replace('`', '"')
    
    # Извлекаем только тело триггера (между BEGIN и END)
    begin_match = re.search(r'BEGIN(.*?)END', body, flags=re.IGNORECASE | re.DOTALL)
    if begin_match:
        trigger_body = begin_match.group(1).strip()
    else:
        # Если нет явных BEGIN/END, берем все после FOR EACH ROW
        each_row_match = re.search(r'FOR EACH ROW\s*(.*)', body, flags=re.IGNORECASE | re.DOTALL)
        if each_row_match:
            trigger_body = each_row_match.group(1).strip()
        else:
            trigger_body = body
    
    # Убираем точку с запятой в конце если есть
    trigger_body = re.sub(r';\s*$', '', trigger_body)
    
    # Конвертируем SET NEW.col = val -> NEW.col := val;
    trigger_body = re.sub(r'SET\s+NEW\.(\w+)\s*=\s*(.+?);', r'NEW.\1 := \2;', trigger_body, flags=re.IGNORECASE)
    
    # Для простых SET без BEGIN/END
    trigger_body = re.sub(r'SET\s+NEW\.(\w+)\s*=\s*(.+?)$', r'NEW.\1 := \2;', trigger_body, flags=re.IGNORECASE)
    
    # Конвертируем IF(cond, val1, val2) -> CASE WHEN cond THEN val1 ELSE val2 END
    def replace_if(match):
        cond, val1, val2 = match.group(1), match.group(2), match.group(3)
        return f'CASE WHEN {cond} THEN {val1} ELSE {val2} END'
    trigger_body = re.sub(r'IF\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)', replace_if, trigger_body, flags=re.IGNORECASE)
    
    # NOW() -> CURRENT_TIMESTAMP
    trigger_body = trigger_body.replace('NOW()', 'CURRENT_TIMESTAMP')
    
    # Добавляем недостающие END IF для IF конструкций
    if 'IF' in trigger_body.upper() and 'END IF' not in trigger_body.upper():
        # Простая эвристика - если есть IF и THEN, но нет END IF, добавляем его
        lines = trigger_body.split('\n')
        if_lines = [i for i, line in enumerate(lines) if 'IF' in line.upper() and 'THEN' in line.upper()]
        if if_lines:
            # Добавляем END IF в конец
            trigger_body += '\n    END IF;'
    
    # Убедимся, что есть точка с запятой в конце
    if not trigger_body.strip().endswith(';'):
        trigger_body += ';'
    
    return trigger_body.strip()

def get_return_value(event_manipulation):
    """Определяет что возвращать в зависимости от типа триггера"""
    if event_manipulation.upper() == 'DELETE':
        return 'OLD'
    else:
        return 'NEW'

def migrate_triggers(mysql_db, pg_db):
    print("🚀 Начало миграции триггеров...")

    # Подключение к MySQL
    mysql_conn = mysql.connector.connect(
        host="localhost",
        user="migrator",
        password="migrator123",
        database=mysql_db
    )
    mysql_cursor = mysql_conn.cursor()

    # Подключение к PostgreSQL
    pg_conn = psycopg2.connect(
        host="localhost",
        database=pg_db,
        user="postgres",
        password="893476"
    )
    pg_cursor = pg_conn.cursor()

    # Получаем список триггеров
    mysql_cursor.execute(f"""
        SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, ACTION_TIMING, ACTION_STATEMENT
        FROM information_schema.triggers
        WHERE TRIGGER_SCHEMA = '{mysql_db}'
    """)
    triggers = mysql_cursor.fetchall()
    print(f"📊 Найдено триггеров: {len(triggers)}")

    for trigger in triggers:
        trigger_name, table_name, event, timing, action_statement = trigger
        try:
            # Получаем полное определение триггера
            mysql_cursor.execute(f"SHOW CREATE TRIGGER `{trigger_name}`")
            trigger_row = mysql_cursor.fetchone()
            if not trigger_row:
                print(f"⚠️ Триггер {trigger_name} не найден, пропускаем")
                continue
                
            trigger_sql = trigger_row[2]
            print(f"🔍 Обрабатываем триггер: {trigger_name}")
            print(f"📝 Исходный SQL: {trigger_sql}")

            # Извлекаем тело триггера
            body_pg = extract_trigger_body(trigger_sql, trigger_name, event)
            print(f"🔄 Конвертированное тело: {body_pg}")

            # Определяем что возвращать
            return_value = get_return_value(event)

            # Создаём уникальное имя функции
            func_name = f"{trigger_name}_func"

            # Создаём функцию триггера
            create_func = f"""
CREATE OR REPLACE FUNCTION "{func_name}"() 
RETURNS TRIGGER AS $$
BEGIN
    {body_pg}
    RETURN {return_value};
END;
$$ LANGUAGE plpgsql;
"""
            
            print(f"📋 Создаём функцию: {create_func}")
            pg_cursor.execute(create_func)

            # Создаём триггер
            create_trig = f"""
DROP TRIGGER IF EXISTS "{trigger_name}" ON "{table_name}";
CREATE TRIGGER "{trigger_name}"
    {timing} {event} ON "{table_name}"
    FOR EACH ROW
    EXECUTE FUNCTION "{func_name}"();
"""
            
            print(f"📋 Создаём триггер: {create_trig}")
            pg_cursor.execute(create_trig)
            pg_conn.commit()
            print(f"✅ Триггер перенесён: {trigger_name}")

        except Exception as e:
            print(f"❌ Ошибка при миграции триггера {trigger_name}: {e}")
            import traceback
            traceback.print_exc()
            pg_conn.rollback()

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()
    print("\n🎉 Миграция триггеров завершена!")

if __name__ == "__main__":
    migrate_triggers(mysql_db="sakila", pg_db="sakila_pg")