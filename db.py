import sqlite3

DB = "bonus.db"

def init_db():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY,
        points INTEGER DEFAULT 0
    )
    """)

    conn.commit()
    conn.close()
