import mysql.connector
import logging

db_logger = logging.getLogger("db_queries")
db_logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("database.log",encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(message)s'))
db_logger.addHandler(file_handler)


error_handler = logging.FileHandler("database_error.log",encoding='utf-8')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
db_logger.addHandler(error_handler)


db_logger.propagate = False

def escape_value(val):
    if val is None:
        return "NULL"
    
    val = str(val).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{val}'"

def connect():
    try:
      conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="pdp_new" 
        )
      return conn
    except Exception as e:
        db_logger.error(f"Error in connect(): {e}", exc_info=True)
        return None

def create(table_name: str):
    try:
        queary = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                restaurant_id varchar(50)  NOT NULL,
                restaurant_name varchar(255),
                restaurant_cuisine varchar(255),
                restaurant_image TEXT,
                restaurant_timezone varchar(50),
                restaurant_opening_hours TEXT,
                restaurant_menu LONGTEXT,
                PRIMARY KEY (restaurant_id)
                ); """
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(queary)
        conn.commit()
    except Exception as e:
        db_logger.error(f"Error in create(): {e}", exc_info=True)

def insert_into_db(table_name: str, data: dict):
    try:
        cols = ",".join(list(data.keys()))
        vals = ",".join(['%s'] * len(data.keys()))
        queary = f"""INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"""
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(queary, tuple(data.values()))
        conn.commit()
    except Exception as e:
        db_logger.error(f"Error in insert_into_db(): {e}", exc_info=True)

def batch_insert(table_name: str, rows: list[dict]):
    if not rows:
        return
    try:
        cols = ",".join(rows[0].keys())
        vals = ",".join(['%s'] * len(rows[0].keys()))
        query = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"

        conn = connect()
        cursor = conn.cursor()

        cursor.executemany(query, [tuple(r.values()) for r in rows])
        conn.commit()

    
        inserted_count = cursor.rowcount

    
        inserted_rows = rows[:inserted_count]
        for r in inserted_rows:
            escaped_vals = ",".join(escape_value(v) for v in r.values())
            log_query = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({escaped_vals});"
            db_logger.info(log_query)

        cursor.close()
        conn.close()
    except Exception as e:
        db_logger.error(f"Error in batch_insert(): {e}", exc_info=True)
    
if __name__ == "__main__":
    connect()