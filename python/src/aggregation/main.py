import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

MESSAGE_FIELDS = 3


class AggregationFilter:
    def __init__(self):
        self._clients_fruit_top = {}
        self._clients_eof_count = {}
        try:
            self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
            )
            self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )
        except:
            self.close()
            raise

    def close(self):
        try:
            if self.input_exchange:
                self.input_exchange.close()
        finally:
            self.input_exchange = None

        try:
            if self.output_queue:
                self.output_queue.close()
        finally:
            self.output_queue = None

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data message: {client_id},{fruit},{amount}")
        fruit_top = self._clients_fruit_top.setdefault(client_id, [])
        top = fruit_item.FruitItem(fruit, amount)
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                top += fruit_top.pop(i)
                break
        bisect.insort(fruit_top, top)

    def _process_eof(self, client_id):
        logging.info(f"Received EOF: {client_id}")
        self._clients_eof_count[client_id] = (
            self._clients_eof_count.get(client_id, 0) + 1
        )
        if self._clients_eof_count[client_id] < SUM_AMOUNT:
            return

        fruit_top = self._clients_fruit_top.get(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )

        self.output_queue.send(
            message_protocol.internal.serialize([client_id, fruit_top])
        )

        self._clients_eof_count.pop(client_id)
        self._clients_fruit_top.pop(client_id)

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Process message")
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == MESSAGE_FIELDS:
                self._process_data(*fields)
            else:
                self._process_eof(*fields)
            ack()
        except:
            logging.error("Couldn't process message")
            nack()

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            self.close()

    def shutdown(self, *args):
        if self.input_exchange:
            self.input_exchange.stop_consuming()

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGINT, aggregation_filter.shutdown)
    signal.signal(signal.SIGTERM, aggregation_filter.shutdown)
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
