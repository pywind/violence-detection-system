import sys

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

from config import configuration


class KafkaHandler:
    def __init__(self, is_producer=False, is_consumer=False):
        self.topic = configuration['CLOUDKARAFKA_TOPIC'].split(',')

        self.__producer_config = {
            'bootstrap.servers': configuration['CLOUDKARAFKA_BROKERS'],
            'session.timeout.ms': configuration['SESSION_TIMEOUT'],
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'security.protocol': configuration['SECURITY_PROTOCOL'],
            'sasl.mechanisms': configuration['SASL_MECHANISM'],
            'sasl.username': configuration['CLOUDKARAFKA_USERNAME'],
            'sasl.password': configuration['CLOUDKARAFKA_PASSWORD']
        }

        self.__consumer_config = {
            'bootstrap.servers': configuration['CLOUDKARAFKA_BROKERS'],
            'group.id': "%s-consumer" % configuration['CLOUDKARAFKA_USERNAME'],
            'session.timeout.ms': configuration['SESSION_TIMEOUT'],
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'security.protocol': configuration["SECURITY_PROTOCOL"],
            'sasl.mechanisms': configuration["SASL_MECHANISM"],
            'sasl.username': configuration['CLOUDKARAFKA_USERNAME'],
            'sasl.password': configuration['CLOUDKARAFKA_PASSWORD']
        }
        if is_producer:
            self.producer = Producer(**self.__producer_config)
        if is_consumer:
            self.consumer = Consumer(**self.__consumer_config)

    @staticmethod
    def delivery_callback(err, msg):
        """
        Reports the Failure or Success of a message delivery.
        Args:
            err  (KafkaError): The Error that occurred while message producing.
            msg    (Actual message): The message that was produced.
        Note:
            In the delivery report callback the Message.key() and Message.value()
            will be the binary format as encoded by any configured Serializers and
            not the same object that was passed to produce().
            If you wish to pass the original object(s) for key and value to delivery
            report callback we recommend a bound callback or lambda where you pass
            the objects along.
        """
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' %
                             (msg.topic(), msg.partition()))

    def produce_message(self, msg):
        try:
            self.producer.produce(self.topic[0], msg, callback=self.delivery_callback)
        except BufferError:
            sys.stderr.write(
                '%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(self.producer))

        self.producer.poll(0)
        # sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer))
        self.producer.flush()

    def consume_messages(self):
        self.consumer.subscribe(self.topic)
        try:
            while True:
                msg = self.consumer.poll(timeout=1)
                if msg is None:
                    continue
                if msg.error():
                    # Error or event
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        # Error
                        raise KafkaException(msg.error())
                else:
                    # Proper message
                    sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                     (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))

                    yield msg.value()

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')
        except Exception:
            sys.stderr.write('%% Aborted by program\n')
        # Close down consumer to commit final offsets.
        self.consumer.close()


if __name__ == '__main__':
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    producer_obj = KafkaHandler(is_consumer=True)
    producer_obj.consume_messages()
