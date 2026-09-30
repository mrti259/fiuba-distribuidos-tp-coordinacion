import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:
    def __init__(self):
        try:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )
            self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )
        except:
            self.close()
            raise

    def close(self):
        try:
            if self.input_queue:
                self.input_queue.close()
        finally:
            self.input_queue = None

        try:
            if self.output_queue:
                self.output_queue.close()
        finally:
            self.output_queue = None

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Received top")
            fields = message_protocol.internal.deserialize(message)
            self.output_queue.send(message_protocol.internal.serialize(fields))
            ack()
        except:
            logging.error("Couldn't not process top")
            nack()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            self.close()

    def shutdown(self, *args):
        if self.input_queue:
            self.input_queue.stop_consuming()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGINT, join_filter.shutdown)
    signal.signal(signal.SIGTERM, join_filter.shutdown)
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
