import sqlite3


def create_table(cur: sqlite3.Cursor, con: sqlite3.Connection, execute: str):
    cur.execute(execute)
    con.commit()


def insert(cur: sqlite3.Cursor, con: sqlite3.Connection, execute: str, values: tuple):
    cur.execute(execute, values)
    con.commit()


def select(cur: sqlite3.Cursor, execute: str, values: tuple = None, message: str = None):
    if values is not None:
        cur.execute(execute, values)
    else:
        cur.execute(execute)

    cur_result = cur.fetchall()
    if len(cur_result) == 0 and message is not None:
        return message
    return cur_result


def close(conn: sqlite3.Connection):
    conn.close()
    return "CONNECTION IS CLOSED"
