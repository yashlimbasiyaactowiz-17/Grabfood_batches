import mysql.connector
def connect():

      conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="pdp_new" 
        )
      return conn

def create(table_name: str):
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

def insert_into_db(table_name: str, data: dict):
    cols = ",".join(list(data.keys()))
    vals = ",".join(['%s'] * len(data.keys()))
    queary = f"""INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"""
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(queary, tuple(data.values()))
    conn.commit()

def batch_insert(table_name: str, rows: list[dict]):
    if not rows:
        return
    rows = [r for r in rows if r.get("restaurant_id") is not None]
    if not rows:
        return
    cols = ",".join(list(rows[0].keys()))
    vals = ",".join(['%s'] * len(rows[0].keys()))
    queary = f"""INSERT IGNORE INTO {table_name} ({cols}) VALUES ({vals})"""
    conn = connect()
    cursor = conn.cursor()
    cursor.executemany(queary, [tuple(r.values()) for r in rows])
    conn.commit()

if __name__ == "__main__":
    connect()