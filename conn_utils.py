from airflow.providers.postgres.hooks.postgres import PostgresHook

def connect_to_postgresql():
    try:
        postgres_hook = PostgresHook(postgres_conn_id='Postgres_Staging')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        return cursor
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise

def close_connection(cursor):
    try:
        cursor.close()
    except Exception as e:
        print(f"Error closing PostgreSQL cursor: {e}")
    finally:
        cursor.connection.close()