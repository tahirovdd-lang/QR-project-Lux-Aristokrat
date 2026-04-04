import sqlite3

DB = "bonus.db"

def get_or_create_user(user_id):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("SELECT points FROM users WHERE telegram_id=?", (user_id,))
    row = cur.fetchone()

    if not row:
        cur.execute("INSERT INTO users (telegram_id, points) VALUES (?, ?)", (user_id, 0))
        conn.commit()
        points = 0
    else:
        points = row[0]

    conn.close()
    return points

def add_points(user_id, amount):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("UPDATE users SET points = points + ? WHERE telegram_id=?", (amount, user_id))
    conn.commit()

    cur.execute("SELECT points FROM users WHERE telegram_id=?", (user_id,))
    total = cur.fetchone()[0]

    conn.close()
    return total
