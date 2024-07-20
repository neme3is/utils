import pika


class RabbitMqTestBase:
    _connection = None
    _channel = None
    _routing_key = None
    _ip_address = None

    def __init__(self, ip_addr: str = "10.22.54.5", routing_key: str = "Hello"):
        self._ip_address = ip_addr
        self._routing_key = routing_key

    def create_connection(self):
        credentials = pika.PlainCredentials('postgres', 'wombat')
        self._connection = \
            pika.BlockingConnection(pika.ConnectionParameters(host=self._ip_address, credentials=credentials))
        self._channel = self._connection.channel()


class Publisher(RabbitMqTestBase):

    def create_queue(self, queue_name: str = None):
        if queue_name is None:
            queue_name = self._routing_key
        self._channel.queue_declare(queue=queue_name)

    def send_message_to_queue(self, routing_key: str = None):
        if routing_key is None:
            routing_key = self._routing_key
        self._channel.basic_publish(exchange="", routing_key=routing_key, body=str.encode('Hello World'))

    def close_connection(self):
        self._connection.close()


class Consumer(RabbitMqTestBase):

    @staticmethod
    def _messages_handler(ch, method, properties, body):
        print(f"Received {body}")

    def start_consuming(self):
        self._channel.basic_consume(queue=self._routing_key,
                                    auto_ack=True,
                                    on_message_callback=Consumer._messages_handler)
        self._channel.start_consuming()

    def close_connection(self):
        self._channel.stop_consuming()
        self._connection.close()


if __name__ == "__main__":
    publisher = Publisher()
    publisher.create_connection()
    publisher.create_queue()
    publisher.send_message_to_queue()
    publisher.close_connection()

    consumer = Consumer()
    consumer.create_connection()
    consumer.start_consuming()
    consumer.close_connection()