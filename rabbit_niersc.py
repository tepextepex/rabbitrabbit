import pika
import json


class Rabbit(object):

    def __init__(self, credentials=None, host=None, vhost_name=None, profession=None, name=None, data_type=None):
        """
        :param credentials: tuple (login, password)
        :param host: hostname/ipv4
        :param vhost_name: RabbitMQ virtual host name
        :param profession: 'master', 'agent', 'unit' or 'logger'
        :param name: will listen to a queue with this name. May be not unique
        :param data_type: routing key
        """
        self._setup_defaults(credentials, host, vhost_name, profession, name, data_type)

        self.credentials = pika.PlainCredentials(self.login, self.password)
        self.con_params = pika.ConnectionParameters(self.host,
                                                    5672,
                                                    self.vhost_name,
                                                    self.credentials)
        self.connection = pika.BlockingConnection(self.con_params)
        # self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()

        self._setup_incoming()
        self._setup_outgoing()

        # subscribe the incoming Q for messages with a proper data_type tag only:
        if self.incomingExName is not None and self.incomingQName is not None:  # false for profession=master which doesn't listen to any Qs
            self.channel.queue_bind(exchange=self.incomingExName,
                                    queue=self.incomingQName,
                                    routing_key=self.data_type)

    def _setup_defaults(self, credentials, host, vhost_name, profession, name, data_type):

        self.possible_profs = ("master", "agent", "unit", "logger")  # whitelist for "profession" argument

        if credentials is None:
            credentials = ("guest", "guest")
        self.login = credentials[0]
        self.password = credentials[1]

        if vhost_name is None:
            vhost_name = "/"
        self.vhost_name = vhost_name

        if host is None:
            host = "localhost"
        self.host = host

        if profession is None:
            profession = "agent"
        else:
            try:
                self.possible_profs.index(profession)  # checks if the "profession" value exists in a whitelist
            except ValueError:
                profession = "agent"  # raised if "profession" value is unknown and sets the default
        self.prof = profession

        if name is None:
            name = "test"
        self.name = name

        if data_type is None:
            data_type = "test_data"
        self.data_type = data_type

    def _setup_incoming(self):
        # sorry for camelCase below :)
        if self.prof == "agent":
            self.incomingExName = "to_agent_routing"
            self.incomingQName = "_".join(["to", "agent", self.name])
        elif self.prof == "unit":
            self.incomingExName = "from_agent_routing"
            self.incomingQName = "_".join(["from", "agent", self.name])
        elif self.prof == "logger":
            self.incomingExName = "to_logger_routing"
            self.incomingQName = "to_logger"  # only one "to_logger" Q exists, no need in unique names
        elif self.prof == "master":
            # master doesn't listen to any incoming exchanges and queues
            self.incomingExName = None
            self.incomingQName = None

        if self.incomingExName is not None:
            self.channel.exchange_declare(exchange=self.incomingExName,
                                          exchange_type="direct")

        if self.incomingQName is not None:
            self.channel.queue_declare(queue=self.incomingQName,
                                       durable=True)

    def _setup_outgoing(self):

        if self.prof == "master":
            self.outgoingExName = "to_agent_routing"
            self.outgoingQName = None  # intentionally we won't create any Qs from the side of "master" - they'll be created by "agents"
        elif self.prof == "agent":
            self.outgoingExName = "from_agent_routing"
            self.outgoingQName = "_".join(["from", "agent", self.name])
        elif self.prof == "unit":
            self.outgoingExName = "to_logger_routing"
            self.outgoingQName = "to_logger"
        elif self.prof == "logger":
            # logger does not send any messages
            self.outgoingExName = None
            self.outgoingQName = None

        self.outgoingQName = None  # we won't create any outgoing queues since we send messages into an exchange, not in the queue
        # the outgoing exchange will sort our messages and deliver them into the queues created by Rabbits who listen to them

        if self.outgoingExName is not None:
            self.channel.exchange_declare(exchange=self.outgoingExName,
                                          exchange_type="direct")

        # this piece of code below will never work since self.outgoingQName is always None:
        if self.outgoingQName is not None:
            self.channel.queue_declare(queue=self.outgoingQName,
                                       durable=True)

    def duty(self, **kwargs):
        """
        This method is called each time the Rabbit instance receives a message.
        **kwargs are filled from the corresponding fields in the message.
        Assign your own function to this method.
        """
        print "I have nothing to do."
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
        self.say(body)

    def say(self, msg):

        self.channel.basic_publish(exchange=self.outgoingExName,
                                   routing_key=self.data_type,
                                   body=msg)

    def report_settings(self):
        return "[%s rabbit] Listening to: %s (receiving messages from %s exchange by %s tag);\nsending to: %s exchange." \
               % (self.name, self.incomingQName, self.incomingExName, self.data_type, self.outgoingExName)

    def run(self):

        if self.prof != "master":
            self.channel.basic_consume(self.on_request,
                                       queue=self.incomingQName,
                                       no_ack=False)
            print self.report_settings()
            print("Awaiting requests")
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
