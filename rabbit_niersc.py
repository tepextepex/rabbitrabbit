import pika
import json
import uuid


class Rabbit(object):

    def __init__(self, host=None, profession=None, name=None, data_type=None):

        self._setup_defaults(host, profession, name, data_type)

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()

        self._setup_incoming()
        self._setup_outgoing()

        # subscribe the incoming Q for messages with a proper data_type tag only:
        self.channel.queue_bind(exchange=self.incomingExName,
                                queue=self.incomingQName,
                                routing_key=self.data_type)

    def _setup_defaults(self, host, profession, name, data_type):

        if host is None:
            host = "localhost"
        self.host = host

        if profession is None:
            profession = "agent"
        self.profession = profession

        if name is None:
            name = "test"
        self.name = name

        if data_type is None:
            data_type = "test_data"
        self.data_type = data_type

    def _setup_incoming(self):

        self.incomingExName = "to_agent_routing" if self.profession == "agent" else "from_agent_routing"
        self.channel.exchange_declare(exchange=self.incomingExName,
                                      exchange_type="direct")

        self.incomingQName = "_".join(["to", "agent", self.name])
        self.channel.queue_declare(queue=self.incomingQName,
                                   durable=True)

    def _setup_outgoing(self):

        self.outgoingExName = "from_agent_routing" if self.profession == "agent" else "to_logger_routing"
        self.channel.exchange_declare(exchange=self.outgoingExName,
                                      exchange_type="direct")

        self.outgoingQName = "_".join(["from", "agent", self.name]) if self.profession == "agent" else "to_logger"
        self.channel.queue_declare(queue=self.outgoingQName,
                                   durable=True)

    def duty(self, **kwargs):
        """
        This method is called each time the Rabbit instance receives a message.
        **kwargs are filled from the corresponding fields in the message.
        Assign your own function to this method.
        """
        print "I have nothing to do. I am just re-routing messages."
        pass

    def on_request(self, ch, method, properties, body):

        print "Got message: key = %s, body = %s" % (method.routing_key, body)
        # let's acknowledge the incoming msg first:
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        # Here we should do all the work (processing and so on)
        msg_json = json.loads(body)
        # TODO: pass inside the following function every existing JSON item as kwarg:
        self.duty(a=msg_json["a"], b=msg_json["b"])
        # Then send a message further
        self.channel.basic_publish(exchange=self.outgoingExName,
                                   routing_key=self.data_type,
                                   body=body)

    def run(self):

        self.channel.basic_consume(self.on_request,
                                   queue=self.incomingQName,
                                   no_ack=False)
        print("Awaiting requests from '%s' exchange with data_type tag '%s'" % (self.incomingExName, self.data_type))
        self.channel.start_consuming()

    '''
    def stop(self):
        self.channel.stop_consuming()
        self.connection.close()
    '''


################################################################################
#                              Let's test it!                                  #
################################################################################
if __name__ == "__main__":
    r = Rabbit(profession="agent")
    # r = Rabbit(profession="unit")
    r.run()
