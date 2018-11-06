from psycopg2.extensions import AsIs
import psycopg2
import json

def db_insert_geojson(conn, tbl_name, name, type, json_file):
    ''' Insert a geojson to a database '''
    qry = "INSERT INTO monitoring.public.%(table)s (name, type, geo_extent) " \
          "VALUES (%(name)s, %(type)s, ST_SetSRID(ST_GeomFromGeoJSON(%(gj)s), 4326));"
    cur = conn.cursor()
    # Get geo data from geojson file
    gj = json_file['geometry']
    print name
    print gj
    cur.execute(qry, ({"table": AsIs(tbl_name), "name": name,
                       "type": type, "gj": json.dumps(gj)}),)
    conn.commit()

def create_tbl(conn):
    ''' Create a Table '''
    # UNIQUE
    )
    sql = "CREATE TABLE tbl_geo_test (id SERIAL," \
          "                 datetime timestamp with time zone," \
          "                 name char(200)," \
          "                 type char(70)," \
          "                 geo_extent geometry(Polygon,4326)," \
          "                 CONSTRAINT tbl_geo_test_name_type PRIMARY KEY (name, type)" \
          ")" \
          "TABLESPACE pg_default;" \
          "ALTER TABLE tbl_geo_test OWNER to postgres;" \
          "CREATE INDEX idx_tbl_geo_test ON tbl_geo_test (datetime);"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit(
def clean_tbl(conn, tbl_name):
    ''' Clean table '''
    sql = "DELETE FROM %s" % tbl_name
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

# Database credits
db_ip = '192.168.0.53'
dbname = 'monitoring'
user = 'postgres'
password = 'postgres'

conn_string = "host='%s' dbname='%s' " \
              "user='%s' password='%s'" %\
              (db_ip, dbname, user, password)

conn = psycopg2.connect(conn_string)
#create_tbl(conn)

with open('data.json') as f:
    data = json.load(f)
    try:
        db_insert_geojson(conn, 'tbl_geo_test', 'name', 'type', data)
    except:
        conn.rollback()
        conn.close()
        conn = psycopg2.connect(conn_string)

conn.close()