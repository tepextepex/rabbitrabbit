import pika
import json
from datetime import datetime
from random import randint

data_type = "test_data"
# data_type = "wrong_data"

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

# the following elements should always exist in the message, even if they're empty
payload = {
    "a": randint(1, 6),
    "b": randint(1, 6),
    "time": str(datetime.now().time()),
    "sourcedata": "/home/tepex/NIERSC/rabbitmq/RPC-2/data/niersc_ice_class_3d_20180720.tif",
    "outdir": "/home/tepex/NIERSC/rabbitmq/RPC-2/data/"
}
message = json.dumps(payload)

# publishes the request message, with two properties: reply_to and correlation_id
channel.basic_publish(exchange='to_agent_routing',
                      routing_key=data_type,
                      body=message)
print("Message sent")
