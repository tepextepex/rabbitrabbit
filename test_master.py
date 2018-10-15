from rabbit_niersc import Rabbit
import json
from datetime import datetime
from random import randint

data_type = "test_data"
# data_type = "wrong_data"

r = Rabbit(host="localhost", profession="master", name="monitoring_unit", data_type="test_data")

# the following elements should always exist in the message, even if they're empty
payload = {
    "a": randint(1, 6),
    "b": randint(1, 6),
    "time": str(datetime.now().time()),
    "sourcedata": "/home/tepex/NIERSC/rabbitmq/RPC-2/data/niersc_ice_class_3d_20180720.tif",
    "outdir": "/home/tepex/NIERSC/rabbitmq/RPC-2/data/"
}
message = json.dumps(payload)

# notice that you don't need to run() the master rabbit, because it doesn't listen to any queues
# but it can send messages to agents:
r.say(message)

print("Message sent")
