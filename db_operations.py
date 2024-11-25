import logging
from conn_utils import close_connection,connect_to_postgresql

def process_data(df, table, column_mappings,conflict_columns):
    """Inserts data into a given database table from a DataFrame based on dynamic column mappings."""
    logging.info(f"Inserting data into the database table: {table}")
    columns = ", ".join(column_mappings.values())
    placeholders = ", ".join(['%s'] * len(column_mappings))
    key_columns = ", ".join(f"{col}=EXCLUDED.{col}" for col in column_mappings.values() if col not in conflict_columns)

    #Building dynamic query based on given arguments
    conflict_clause = ", ".join(conflict_columns)
    sql = f"""
        INSERT INTO "Propstack_Test". {table} ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {key_columns};
    """
    
    try:
        conn = connect_to_postgresql()
        for _, row in df.iterrows():
            values = tuple(row[key] for key in column_mappings.keys())
            conn.execute(sql, values)
        conn.connection.commit()
        logging.info(f"Successfully inserted/updated data into {table}.")
    except Exception as e:
        logging.error(f"Error inserting/updating data into {table}: {e}")
        conn.connection.rollback()
    finally:
        if conn:
            close_connection(conn)
