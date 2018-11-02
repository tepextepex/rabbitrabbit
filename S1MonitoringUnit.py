from rabbit_niersc import Rabbit
from datetime import datetime
from auth import auth_data, host, vhost_name
from auth import db_auth_data, db_host, db_name, db_table
from auth import storage
from MonitoringUtils import *  # allows using its functions without prefixing them with the module name. But be aware of namespace collisions!

credentials = auth_data["cordelia"]
data_type = "S1_zip"  # this will be the routing key for our messages

r = Rabbit(credentials=credentials,
           host=host,
           vhost_name=vhost_name,
           profession="master",
           name="monitoring_unit",
           data_type=data_type)


new_products = seek_new_products(storage, "S1*.zip", db_host, db_name, db_auth_data[0], db_auth_data[1], db_table)

for product in new_products:
    payload = {
        "time": str(datetime.now().time()),
        "source_data": product,
        "out_dir": storage,
        "options": None
    }
    print "Generated message: %s" % payload
    r.say(payload)

# notice that you don't need to run() the master rabbit, because it doesn't listen to any queues
