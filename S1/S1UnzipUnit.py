"""
Receives: file name of Sentinel-1 zip-archived product.
Produces: GeoTiff(s)
"""
from rabbit_niersc import Rabbit
from conf.auth import auth_data, host, vhost_name
from S1L1Tools import S1L1Tools
import os.path

credentials = auth_data["ophelia"]

r = Rabbit(credentials=credentials,
           host=host,
           vhost_name=vhost_name,
           profession="unit",
           name="S1UnzipUnit",
           data_type="S1_zip")


def unpack(**msg):
    """
    Always use the same kwarg names as defined in the structure of JSON-message.
    On function call kwarg values will be assigned according to received message.
    The following kwargs are available:
        - msg["time"]
        - msg["source_data"]
        - msg["out_dir"]
        - msg["options"]
    """
    try:
        source_data = msg["source_data"]
        out_dir = msg["out_dir"]
        full_path = os.path.join(out_dir, source_data)
        s = S1L1Tools(full_path)
        s.export_to_l2(out_dir, projection='+proj=longlat +datum=WGS84 +no_defs', x_scale=1, y_scale=1)
    except OSError as e:  # more likely this is "FileNotFoundError" - wrong source_data path
        print str(e)
        pass
    except Exception as e:  # and these are all other exceptions
        print str(e)
        pass


r.duty = unpack

r.run()
