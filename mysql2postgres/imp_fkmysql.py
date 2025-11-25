import mysql.connector
import psycopg2

def migrate_foreign_keys_with_auto_index(mysql_db, pg_db):
    print("🚀 Начало миграции внешних ключей...")

    mysql_conn = mysql.connector.connect(
        host="localhost",
        user="migrator",
        password="migrator123",
        database=mysql_db
    )
    mysql_cursor = mysql_conn.cursor(dictionary=True)

    pg_conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="893476",
        database=pg_db
    )
    pg_cursor = pg_conn.cursor()

    mysql_cursor.execute(f"""
        SELECT
            kcu.CONSTRAINT_NAME AS fk_name,
            kcu.TABLE_NAME AS child_table,
            kcu.COLUMN_NAME AS child_column,
            kcu.REFERENCED_TABLE_NAME AS parent_table,
            kcu.REFERENCED_COLUMN_NAME AS parent_column,
            rc.UPDATE_RULE AS on_update,
            rc.DELETE_RULE AS on_delete
        FROM information_schema.KEY_COLUMN_USAGE kcu
        JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
          ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
         AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
        WHERE kcu.TABLE_SCHEMA = %s
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL;
    """, (mysql_db,))

    fkeys = mysql_cursor.fetchall()
    print(f"📊 Найдено внешних ключей: {len(fkeys)}")

    added = 0
    for fk in fkeys:
        fk_name = fk["fk_name"]
        child_table = fk["child_table"]
        child_col = fk["child_column"]
        parent_table = fk["parent_table"]
        parent_col = fk["parent_column"]
        on_update = fk["on_update"].replace("RESTRICT", "NO ACTION")
        on_delete = fk["on_delete"].replace("RESTRICT", "NO ACTION")

        try:
            # Проверяем наличие PK/UNIQUE в родительской таблице
            pg_cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu
                  ON tc.constraint_name = ccu.constraint_name
                WHERE tc.table_name = %s
                  AND ccu.column_name = %s
                  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE');
            """, (parent_table, parent_col))
            has_unique = pg_cursor.fetchone()[0] > 0

            if not has_unique:
                print(f"⚙️ Добавляем UNIQUE на {parent_table}.{parent_col} (для FK {fk_name})")
                pg_cursor.execute(f"""
                    ALTER TABLE "{parent_table}"
                    ADD CONSTRAINT "uniq_{parent_table}_{parent_col}" UNIQUE ("{parent_col}");
                """)
                pg_conn.commit()

            # Добавляем внешний ключ
            sql = f"""
ALTER TABLE "{child_table}"
ADD CONSTRAINT "{fk_name}"
FOREIGN KEY ("{child_col}")
REFERENCES "{parent_table}" ("{parent_col}")
ON UPDATE {on_update}
ON DELETE {on_delete};
"""
            pg_cursor.execute(sql)
            pg_conn.commit()
            print(f"✅ Добавлен FK {fk_name}: {child_table}.{child_col} → {parent_table}.{parent_col}")
            added += 1

        except Exception as e:
            pg_conn.rollback()
            print(f"⚠️ Ошибка при добавлении {fk_name}: {e}")

    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()

    print(f"\n🎉 Миграция внешних ключей завершена!")
    print(f"📈 Успешно добавлено: {added} из {len(fkeys)} связей.")


if __name__ == "__main__":
    migrate_foreign_keys_with_auto_index("sakila", "sakila_pg")
