from rabbit_niersc import Rabbit
from conf.auth import auth_data, host, vhost_name
import shutil

credentials = auth_data["ophelia"]

r = Rabbit(credentials=credentials,
           host=host,
           vhost_name=vhost_name,
           profession="unit",
           name="S1NGWUnit",
           data_type="S1_render")


def publish(**msg):
    """
    Always use the same kwarg names as defined in the structure of JSON-message.
    On function call kwarg values will be assigned according to received message.
    The following kwargs are available:
        - msg["time"]
        - msg["source_data"]
        - msg["out_dir"]
        - msg["options"]
    """
    result = None
    try:
        source_data = msg["source_data"]
        out_dir = msg["out_dir"]
        shutil.copy2(out_dir + source_data, out_dir + "NGW/" + source_data)
    except TypeError:
        pass
    return result


r.duty = publish

r.run()
