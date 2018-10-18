import os
import os.path
import psycopg2 as psy


def seek_new_products(source_dir, host, dbname, login, pwd, db_table):
    """
    Loops through specified directory and searches for new products ("new" means "is not in the database yet").

    :returns: list of filenames or empty list if nothing found
    """
    new = []
    conn_string = "host=%s dbname=%s user=%s password=%s" % (host, dbname, login, pwd)
    conn = psy.connect(conn_string)
    cur = conn.cursor()

    if os.path.exists(source_dir):
        for fname in os.listdir(source_dir):
            if not is_in_db(cur, fname, db_table):
                new.append(fname)
    cur.close()
    conn.close()

    return new


def is_in_db(cursor, fname, db_table):
    """
    Searches a specified filename in the database.

    :returns: True if a filename was found in the DB, and False if not
    """
    cursor.execute("SELECT * FROM %s WHERE product_id = '%s'" % (db_table, fname))
    return True if cursor.fetchone() is not None else False
