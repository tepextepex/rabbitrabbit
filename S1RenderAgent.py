from rabbit_niersc import Rabbit
from auth import auth_data, host, vhost_name

credentials = auth_data["cordelia"]

r = Rabbit(credentials=credentials,
           host=host,
           vhost_name=vhost_name,
           profession="agent",
           name="S1_zip",
           data_type="S1_zip")


def forward_message(**msg):
    """
    Always use the same kwarg names as defined in the structure of JSON-message.
    On function call kwarg values will be assigned according to received message.
    The following kwargs are available:
        - msg["time"]
        - msg["source_data"]
        - msg["out_dir"]
        - msg["options"]
    """
    payload = {
        "time": msg["time"],
        "source_data": msg["source_data"],
        "out_dir": msg["out_dir"],
        "options": msg["options"]
    }
    r.say(payload)


r.duty = forward_message

r.run()
