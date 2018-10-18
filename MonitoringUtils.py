import os
import os.path
import psycopg2 as psy
import glob


def seek_new_products(source_dir, pattern, host, dbname, login, pwd, db_table):
    """
    Loops through specified directory and searches for new products by pattern ("new" means "is not in the database yet").

    :returns: list of filenames or empty list if nothing found
    """
    new = []
    conn_string = "host=%s dbname=%s user=%s password=%s" % (host, dbname, login, pwd)
    conn = psy.connect(conn_string)
    cur = conn.cursor()

    if os.path.exists(source_dir):
        names = [os.path.basename(x) for x in glob.glob(os.path.join(source_dir, pattern))]  # returns a list containing file names
        for fname in names:
            if not is_in_db(cur, fname, db_table):
                new.append(fname)
    else:
        print "Can't find directory: %s" % source_dir
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
