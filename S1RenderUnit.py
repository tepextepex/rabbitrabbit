from rabbit_niersc import Rabbit
from auth import auth_data, host, vhost_name
from S1L1Tools import S1L1Tools

credentials = auth_data["ophelia"]

r = Rabbit(credentials=credentials,
           host=host,
           vhost_name=vhost_name,
           profession="unit",
           name="S1RenderUnit",
           data_type="S1_zip")


def render(**msg):
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
        s = S1L1Tools(out_dir + source_data)
        s.render(out_dir, ["HH"])
    except TypeError:
        pass
    return result


r.duty = render

r.run()
