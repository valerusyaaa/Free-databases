import pyodbc
import psycopg2
import time


def get_table_structure_with_schema(mssql_conn, table_name):
    """Получает структуру таблицы из SQL Server с учетом схемы"""
    cursor = mssql_conn.cursor()

    # Сначала находим схему таблицы
    schema_query = """
    SELECT 
        SCHEMA_NAME(t.schema_id) AS schema_name,
        t.name AS table_name
    FROM sys.tables t
    WHERE t.name = ?
    """

    cursor.execute(schema_query, (table_name,))
    schema_result = cursor.fetchone()

    if not schema_result:
        print(f"   ❌ Таблица {table_name} не найдена в SQL Server")
        return None, None, None

    schema_name = schema_result.schema_name
    full_table_name = f"{schema_name}.{table_name}" if schema_name != 'dbo' else table_name

    # Получаем информацию о колонках
    query = """
    SELECT 
        c.name AS column_name,
        ty.name AS data_type,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable,
        c.is_identity,
        CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key
    FROM sys.tables t
    INNER JOIN sys.columns c ON t.object_id = c.object_id
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    LEFT JOIN (
        SELECT ic.object_id, ic.column_id
        FROM sys.index_columns ic
        INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE i.is_primary_key = 1
    ) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
    WHERE t.name = ? AND SCHEMA_NAME(t.schema_id) = ?
    ORDER BY c.column_id
    """

    cursor.execute(query, (table_name, schema_name))
    columns = cursor.fetchall()

    # Получаем первичный ключ
    pk_query = """
    SELECT COL_NAME(ic.object_id, ic.column_id) AS pk_column
    FROM sys.index_columns ic
    INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    WHERE t.name = ? AND SCHEMA_NAME(t.schema_id) = ? AND i.is_primary_key = 1
    ORDER BY ic.key_ordinal
    """

    cursor.execute(pk_query, (table_name, schema_name))
    pk_columns = [row[0] for row in cursor.fetchall()]

    cursor.close()
    return columns, pk_columns, schema_name


def create_table_in_postgresql(pg_cursor, table_name, columns, pk_columns, schema_name=None):
    """Создает таблицу в PostgreSQL"""

    # Формируем колонки для CREATE TABLE
    column_definitions = []
    for col in columns:
        column_name = col.column_name
        data_type = map_sql_server_to_postgresql(
            col.data_type,
            col.max_length,
            col.precision,
            col.scale
        )
        nullable = "NOT NULL" if not col.is_nullable else ""

        column_def = f"{column_name} {data_type} {nullable}".strip()
        column_definitions.append(column_def)

    # Формируем PRIMARY KEY
    pk_clause = ""
    if pk_columns:
        pk_clause = f", PRIMARY KEY ({', '.join(pk_columns)})"

    # Создаем таблицу
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(column_definitions)}
        {pk_clause}
    )
    """

    pg_cursor.execute(create_sql)
    print(f"   ✅ Таблица {table_name} создана")


def map_sql_server_to_postgresql(data_type, max_length, precision, scale):
    """Конвертирует типы данных SQL Server -> PostgreSQL"""
    type_map = {
        'int': 'INTEGER',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',
        'bit': 'BOOLEAN',
        'float': 'DOUBLE PRECISION',
        'real': 'REAL',
        'decimal': f'DECIMAL({precision},{scale})',
        'numeric': f'DECIMAL({precision},{scale})',
        'money': 'DECIMAL(19,4)',
        'smallmoney': 'DECIMAL(10,4)',
        'char': f'CHAR({max_length})',
        'varchar': f'VARCHAR({max_length if max_length > 0 else 255})',
        'text': 'TEXT',
        'nchar': f'CHAR({max_length // 2})',
        'nvarchar': f'VARCHAR({max_length // 2 if max_length > 0 else 255})',
        'ntext': 'TEXT',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'datetime2': 'TIMESTAMP',
        'smalldatetime': 'TIMESTAMP',
        'time': 'TIME',
        'timestamp': 'BYTEA',
        'binary': 'BYTEA',
        'varbinary': 'BYTEA',
        'image': 'BYTEA',
        'uniqueidentifier': 'UUID'
    }

    return type_map.get(data_type.lower(), 'TEXT')


def get_all_tables_with_schemas(mssql_conn):
    """Получает все таблицы с их схемами"""
    cursor = mssql_conn.cursor()

    query = """
    SELECT 
        SCHEMA_NAME(schema_id) AS schema_name,
        name AS table_name
    FROM sys.tables 
    WHERE type = 'U'
    ORDER BY schema_name, table_name
    """

    cursor.execute(query)
    tables = cursor.fetchall()
    cursor.close()

    return tables


def check_and_create_tables_with_schemas():
    print("🔐 Проверка и создание таблиц с учетом схем...")

    # Подключения
    mssql_conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=USER\\SQLEXPRESS;'
        'DATABASE=tpcxbb;'
        'Trusted_Connection=yes;'
    )

    pg_conn = psycopg2.connect(
        host="localhost",
        database="tpcxbb_test",
        user="migrator",
        password="migrator123"
    )
    pg_conn.autocommit = True
    pg_cursor = pg_conn.cursor()

    # Получаем все таблицы из SQL Server
    all_tables = get_all_tables_with_schemas(mssql_conn)

    print(f"📊 Найдено таблиц в SQL Server: {len(all_tables)}")

    # Создаем таблицы в PostgreSQL
    created_tables = []
    failed_tables = []

    for schema_name, table_name in all_tables:
        try:
            print(f"\n{'=' * 50}")
            full_table_name = f"{schema_name}.{table_name}" if schema_name != 'dbo' else table_name
            print(f"🔄 Обрабатываем таблицу: {full_table_name}")

            # Получаем структуру из SQL Server
            columns, pk_columns, actual_schema = get_table_structure_with_schema(mssql_conn, table_name)

            if not columns:
                print(f"   ❌ Не удалось получить структуру для {table_name}")
                failed_tables.append(full_table_name)
                continue

            print(f"   📋 Колонок: {len(columns)}, PK: {pk_columns}")

            # Создаем таблицу в PostgreSQL
            create_table_in_postgresql(pg_cursor, table_name, columns, pk_columns, actual_schema)
            created_tables.append(table_name)

        except Exception as e:
            print(f"   ❌ Ошибка при создании {table_name}: {e}")
            failed_tables.append(full_table_name)

    pg_cursor.close()
    mssql_conn.close()
    pg_conn.close()

    print(f"\n🎉 Создание таблиц завершено!")
    print(f"✅ Успешно создано: {len(created_tables)} таблиц")
    print(f"❌ Ошибки: {len(failed_tables)} таблиц")

    return created_tables


def migrate_data_with_schemas(created_tables):
    """Мигрирует данные в созданные таблицы с учетом схем"""
    print(f"\n📦 Начинаем миграцию данных для {len(created_tables)} таблиц...")

    mssql_conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=USER\\SQLEXPRESS;'
        'DATABASE=tpcxbb;'
        'Trusted_Connection=yes;'
    )

    pg_conn = psycopg2.connect(
        host="localhost",
        database="tpcxbb_test",
        user="migrator",
        password="migrator123"
    )
    pg_cursor = pg_conn.cursor()

    success_count = 0

    for table_name in created_tables:
        try:
            print(f"\n{'=' * 50}")
            print(f"🔄 Мигрируем таблицу: {table_name}")

            mssql_cursor = mssql_conn.cursor()

            # Определяем полное имя таблицы с учетом схемы
            schema_query = """
            SELECT SCHEMA_NAME(schema_id) AS schema_name
            FROM sys.tables 
            WHERE name = ?
            """
            mssql_cursor.execute(schema_query, (table_name,))
            schema_result = mssql_cursor.fetchone()

            if schema_result:
                schema_name = schema_result.schema_name
                full_table_name = f"{schema_name}.{table_name}" if schema_name != 'dbo' else table_name
            else:
                full_table_name = table_name

            # Получаем количество строк
            mssql_cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
            total_rows = mssql_cursor.fetchone()[0]
            print(f"   📊 Всего строк: {total_rows:,}")

            if total_rows == 0:
                print("   📭 Таблица пустая, пропускаем")
                mssql_cursor.close()
                continue

            # Очищаем таблицу в PostgreSQL
            pg_cursor.execute(f"TRUNCATE TABLE {table_name}")
            pg_conn.commit()
            print("   ♻️  Таблица очищена")

            # Получаем данные из SQL Server
            mssql_cursor.execute(f"SELECT * FROM {full_table_name}")
            columns = [desc[0] for desc in mssql_cursor.description]
            placeholders = ', '.join(['%s'] * len(columns))

            # Мигрируем данные
            migrated = 0
            batch_size = 1000

            while True:
                rows = mssql_cursor.fetchmany(batch_size)
                if not rows:
                    break

                # Конвертируем специальные типы данных
                processed_rows = []
                for row in rows:
                    processed_row = []
                    for value in row:
                        # Обработка специальных типов
                        if isinstance(value, bytes):
                            processed_row.append(value.hex() if value else None)
                        elif hasattr(value, 'isoformat'):
                            processed_row.append(value.isoformat() if value else None)
                        else:
                            processed_row.append(value)
                    processed_rows.append(tuple(processed_row))

                pg_cursor.executemany(
                    f"INSERT INTO {table_name} VALUES ({placeholders})",
                    processed_rows
                )

                migrated += len(rows)
                progress = (migrated / total_rows) * 100
                print(f"   📦 {migrated:,}/{total_rows:,} ({progress:.1f}%)", end='\r')

            pg_conn.commit()
            print(f"   ✅ Успешно мигрировано: {migrated:,} строк")
            success_count += 1

            mssql_cursor.close()

        except Exception as e:
            print(f"❌ Ошибка при миграции {table_name}: {e}")
            pg_conn.rollback()

    pg_cursor.close()
    mssql_conn.close()
    pg_conn.close()

    print(f"\n🎉 Миграция данных завершена!")
    print(f"✅ Успешно мигрировано: {success_count} таблиц")


if __name__ == "__main__":
    # Сначала создаем таблицы
    created_tables = check_and_create_tables_with_schemas()

    # Потом мигрируем данные
    if created_tables:
        migrate_data_with_schemas(created_tables)
    else:
        print("❌ Нет таблиц для миграции")