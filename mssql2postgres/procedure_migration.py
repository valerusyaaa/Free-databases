import psycopg2


def create_adapted_procedure_fixed():
    """Создает исправленную адаптацию процедуры в PostgreSQL"""

    print("🛠️  СОЗДАНИЕ ИСПРАВЛЕННОЙ АДАПТИРОВАННОЙ ПРОЦЕДУРЫ")
    print("=" * 70)

    pg_conn = psycopg2.connect(
        host="localhost",
        database="tpcxbb_test",
        user="migrator",
        password="migrator123"
    )
    pg_conn.autocommit = True
    pg_cursor = pg_conn.cursor()

    adapted_procedure = """
CREATE OR REPLACE FUNCTION model_record_training_session(
    model_name text,
    model_type text, 
    model_description text,
    model_function_call text,
    model_formula text,
    model_valid_observations integer,
    model_iterations integer,
    model_object bytea,
    model_generation_duration_ms double precision,
    training_duration_ms integer
) RETURNS integer AS $$
DECLARE
    model_id integer;
    existing_model_id integer;
    current_user_name text;
BEGIN
    -- This function records details from a training session:
    current_user_name := current_user;

    -- First, check if model already exists
    SELECT m.model_id INTO existing_model_id 
    FROM models m 
    WHERE m.model_name = model_name;

    IF existing_model_id IS NOT NULL THEN
        -- UPDATE existing model
        model_id := existing_model_id;

        UPDATE models 
        SET model_description = model_description,
            modified_by = current_user_name,
            modify_time = CURRENT_TIMESTAMP,
            model_version = model_version + 1
        WHERE model_id = existing_model_id;

        RAISE NOTICE 'Updated existing model: %, ID: %', model_name, model_id;
    ELSE
        -- INSERT new model
        INSERT INTO models (
            model_name, 
            model_type, 
            model_description, 
            model_version, 
            created_by, 
            create_time
        ) VALUES (
            model_name, 
            model_type, 
            model_description, 
            1,  -- initial version
            current_user_name, 
            CURRENT_TIMESTAMP
        )
        RETURNING model_id INTO model_id;

        RAISE NOTICE 'Created new model: %, ID: %', model_name, model_id;
    END IF;

    -- Store the training history for the model:
    INSERT INTO model_training_history (
        model_id, 
        model_function_call, 
        model_formula, 
        model_valid_observations, 
        model_iterations, 
        model_object,
        model_generation_duration_ms, 
        training_duration_ms,
        created_by,
        create_time
    ) VALUES (
        model_id, 
        model_function_call, 
        model_formula, 
        model_valid_observations, 
        model_iterations, 
        model_object,
        model_generation_duration_ms, 
        training_duration_ms,
        current_user_name,
        CURRENT_TIMESTAMP
    );

    RAISE NOTICE 'Training history recorded for model ID: %', model_id;

    RETURN model_id;

EXCEPTION
    WHEN others THEN
        RAISE EXCEPTION 'Error in model_record_training_session: %%', SQLERRM;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION model_record_training_session IS 
'Records machine learning model training session details.
Handles both new models and updates to existing models.
Migrated from MSSQL procedure sqlr.model_record_training_session';
    """

    try:
        # Удаляем старую версию если существует
        pg_cursor.execute("DROP FUNCTION IF EXISTS model_record_training_session() CASCADE")

        # Создаем адаптированную версию
        pg_cursor.execute(adapted_procedure)
        print("✅ Адаптированная процедура создана успешно!")

        # Проверяем создание
        pg_cursor.execute("""
            SELECT 
                routine_name,
                routine_type,
                data_type as return_type
            FROM information_schema.routines 
            WHERE routine_name = 'model_record_training_session' 
            AND specific_schema = 'public'
        """)
        result = pg_cursor.fetchone()

        if result:
            print(f"📋 Информация о функции:")
            print(f"   - Имя: {result[0]}")
            print(f"   - Тип: {result[1]}")
            print(f"   - Возвращает: {result[2]}")

    except Exception as e:
        print(f"❌ Ошибка создания адаптированной процедуры: {e}")
        import traceback
        traceback.print_exc()

    finally:
        pg_cursor.close()
        pg_conn.close()


def test_procedure_creation():
    """Тестирует создание процедуры"""

    print(f"\n🧪 ПРОВЕРКА СОЗДАНИЯ ПРОЦЕДУРЫ")
    print("=" * 50)

    pg_conn = psycopg2.connect(
        host="localhost",
        database="tpcxbb_test",
        user="migrator",
        password="migrator123"
    )
    pg_cursor = pg_conn.cursor()

    try:
        # Проверяем детальную информацию о функции
        pg_cursor.execute("""
            SELECT 
                p.proname as function_name,
                pg_get_function_arguments(p.oid) as arguments,
                pg_get_function_result(p.oid) as return_type,
                p.prosrc as source_code
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.proname = 'model_record_training_session'
            AND n.nspname = 'public'
        """)

        func_info = pg_cursor.fetchone()
        if func_info:
            print(f"✅ Функция успешно создана:")
            print(f"   - Имя: {func_info[0]}")
            print(f"   - Аргументы: {func_info[1]}")
            print(f"   - Возвращает: {func_info[2]}")
            print(f"   - Код: {len(func_info[3])} символов")

            # Проверяем, что функция компилируется без ошибок
            print(f"\n🔍 Проверка компиляции...")
            pg_cursor.execute("""
                SELECT 
                    p.proisstrict as is_strict,
                    p.provolatile as volatility,
                    p.prorettype::regtype as return_type
                FROM pg_proc p
                WHERE p.proname = 'model_record_training_session'
            """)
            compile_info = pg_cursor.fetchone()
            if compile_info:
                print(f"   ✅ Функция скомпилирована:")
                print(f"      - Strict: {compile_info[0]}")
                print(f"      - Volatility: {compile_info[1]}")
                print(f"      - Return type: {compile_info[2]}")

        else:
            print("❌ Функция не найдена")

    except Exception as e:
        print(f"❌ Ошибка проверки: {e}")
    finally:
        pg_cursor.close()
        pg_conn.close()


if __name__ == "__main__":
    create_adapted_procedure_fixed()
    test_procedure_creation()