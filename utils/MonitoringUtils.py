import os
import os.path
import time
import psycopg2 as psy
# from psycopg2.extensions import AsIs
import glob
import json
import geojson
import shapely.wkt


def seek_new_products(source_dir, pattern, host, dbname, login, pwd, db_table):
    """
    Loops through specified directory and searches for new products by pattern ("new" means "is not in the database yet").

    :returns: list of file names or empty list if nothing found
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
                # r = insert_in_db(cur, fname, db_table)
                # r = db_insert_geojson(conn, db_table, fname, product_type, json_extent)
                # print "INSERT returned %s" % r
                pass
    else:
        print "Can't find directory: %s" % source_dir

    conn.commit()

    cur.close()
    conn.close()

    return new


def is_in_db(cursor, fname, db_table):
    """
    Searches a specified filename in the database.

    :returns: True if a filename was found in the DB, and False if not
    """
    cursor.execute("SELECT * FROM %s WHERE name = '%s'" % (db_table, fname))
    return True if cursor.fetchone() is not None else False


def insert_in_db(cursor, fname, db_table):
    """
    Inserts a specified filename in the database.

    :returns: 1 if fname was successfully inserted or -1 if was not
    """
    # timestamp = datetime.now().timestamp()  # works only for Python 3.x
    timestamp = time.time()
    cursor.execute("INSERT INTO %s (product_id, timestamp) VALUES ('%s', CURRENT_TIMESTAMP)" % (db_table, fname))
    return cursor.rowcount


def log(who, message, host, dbname, login, pwd, db_table):
    """
    Insert the JSON message into database containing log.

    :returns:  1 if the message was successfully logged or -1 if it was not
    """
    conn_string = "host=%s dbname=%s user=%s password=%s" % (host, dbname, login, pwd)
    conn = psy.connect(conn_string)
    cur = conn.cursor()

    json_message = json.dumps(message)
    # timestamp = datetime.now().timestamp()  # works only for Python 3.x
    # timestamp = time.time()

    cur.execute("INSERT INTO %s (who, message, timestamp) VALUES ('%s', '%s', CURRENT_TIMESTAMP)" % (db_table, who, json_message))

    conn.commit()

    cur.close()
    conn.close()

    return cur.rowcount


def save_extent_as_geojson(extent, geojson_path):
    try:
        g1 = shapely.wkt.loads(extent)
        g2 = geojson.Feature(geometry=g1, properties={})
        outfile = open(geojson_path, 'w')
        geojson.dump(g2, outfile)
        outfile.close()
        return True
    except:
        return False


def db_insert_geojson(host, dbname, login, pwd, tbl_name, name, product_type, json_file):
    ''' Insert a geojson to a database '''

    conn_string = "host=%s dbname=%s user=%s password=%s" % (host, dbname, login, pwd)
    conn = psy.connect(conn_string)
    cur = conn.cursor()

    qry = "INSERT INTO monitoring.public.%(table)s (name, type, geo_extent) " \
          "VALUES (%(name)s, %(type)s, ST_SetSRID(ST_GeomFromGeoJSON(%(gj)s), 4326));"
    # Get geo data from geojson file
    gj = json_file['geometry']
    print name
    print gj
    cur.execute(qry, ({"table": AsIs(tbl_name), "name": name,
                       "type": product_type, "gj": json.dumps(gj)}),)
    conn.commit()


# TODO: add different raster formats support
def get_footprint(raster, output_directory):
    """
    Creates GeoJSON file with a footprint for a given GTiff raster

    :param raster: full path to a GeoTIFF raster
    :param output_directory: path to save the resulting GeoJSON
    :return: path for created GeoJSON
    """
    tmp_alpha = raster + "_tmp_alpha.tif"
    geojson_file_name = os.path.join(output_directory, os.path.basename(raster) + "_extent.geojson")

    # Step 1: gdalwarp -dstnodata 0 -dstalpha -of GTiff foo1 foo2 (creates an alpha band using NoData)
    cmd = 'gdalwarp -dstnodata 0 -dstalpha -of GTiff %s %s' % (raster, tmp_alpha)
    os.system(cmd)

    # Step 2: gdal_polygonize.py foo2 -b 2 -f "GeoJSON" foo3 (creates vector file from the alpha band)
    cmd = 'gdal_polygonize.py %s -b 2 -f "GeoJSON" %s' % (tmp_alpha, geojson_file_name)
    os.system(cmd)

    os.remove(tmp_alpha)

    return geojson_file_name
