import mysql.connector
import psycopg2
import re

def convert_mysql_view_sql(sql):
    """
    Конвертирует MySQL SQL для view в PostgreSQL.
    - Обратные кавычки → двойные
    - IFNULL → COALESCE
    - IF(cond, a, b) → CASE WHEN cond THEN a ELSE b END
    - CONCAT(a,b,...) → a || b || ...
    - GROUP_CONCAT(... SEPARATOR ...) → string_agg(..., 'sep' ORDER BY ...)
    - Убирает _utf8mb4
    """
    # Заменяем обратные кавычки
    sql = sql.replace('`', '"')
    # IFNULL -> COALESCE
    sql = sql.replace('IFNULL', 'COALESCE')
    # Убираем _utf8mb4
    sql = re.sub(r'_utf8mb4\'', '\'', sql)
    
    # Конвертируем GROUP_CONCAT(... ORDER BY ... SEPARATOR '...') -> string_agg(..., '...' ORDER BY ...)
    pattern_gc = r'GROUP_CONCAT\((.*?) ORDER BY (.*?) ASC SEPARATOR \'(.*?)\'\)'
    sql = re.sub(pattern_gc, r'string_agg(\1, \3 ORDER BY \2)', sql, flags=re.IGNORECASE)

    # Конвертируем IF(cond, val1, val2) -> CASE WHEN cond THEN val1 ELSE val2 END
    def replace_if(match):
        cond = match.group(1)
        val1 = match.group(2)
        val2 = match.group(3)
        return f'(CASE WHEN {cond} THEN {val1} ELSE {val2} END)'
    pattern_if = r'IF\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)'
    sql = re.sub(pattern_if, replace_if, sql, flags=re.IGNORECASE)

    # Конвертируем CONCAT(a,b,...) -> a || b || ...
    def replace_concat(match):
        args = match.group(1)
        args_list = [arg.strip() for arg in args.split(',')]
        return ' || '.join(args_list)
    pattern_concat = r'CONCAT\s*\(\s*(.+?)\s*\)'
    sql = re.sub(pattern_concat, replace_concat, sql, flags=re.IGNORECASE)

    return sql

def migrate_views_only(mysql_db, pg_db):
    print("🚀 Начало миграции view...")

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

    # Получаем список view
    mysql_cursor.execute(f"""
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = '{mysql_db}'
    """)
    views = [row[0] for row in mysql_cursor.fetchall()]
    print(f"📊 Найдено view: {len(views)}", views)

    for view in views:
        try:
            mysql_cursor.execute(f"SHOW CREATE VIEW `{view}`")
            create_view_sql = mysql_cursor.fetchone()[1]

            # Берём только SELECT часть после AS
            select_index = create_view_sql.upper().find(' AS ')
            view_sql = create_view_sql[select_index + 4:]

            # Конвертируем MySQL SQL → PostgreSQL
            view_sql = convert_mysql_view_sql(view_sql)

            # Создаём view в PostgreSQL
            pg_cursor.execute(f'CREATE OR REPLACE VIEW "{view}" AS {view_sql}')
            pg_conn.commit()
            print(f"✅ View перенесен: {view}")

        except Exception as e:
            print(f"❌ Ошибка при миграции view {view}: {e}")
            pg_conn.rollback()

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()
    print("\n🎉 Миграция view завершена!")

if __name__ == "__main__":
    migrate_views_only(mysql_db="sakila", pg_db="sakila_pg")
