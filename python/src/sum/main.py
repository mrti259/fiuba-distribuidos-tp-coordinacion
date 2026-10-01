import os
import logging
import signal
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# TODO: enviar el tipo en lugar de identificarlo por la cantidad de campos
MESSAGE_FIELDS = 3
EOF_FIELDS = 1
FLUSH_TOKEN_FIELDS = 2


def aggregation_target(fruit: str) -> int:
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT


class SumFilter:
    def __init__(self):
        self._clients_amounts = {}
        self.input_queue = None
        self.data_output_exchanges = []
        try:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )
            for i in range(AGGREGATION_AMOUNT):
                data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
                )
                self.data_output_exchanges.append(data_output_exchange)
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
            if self.data_output_exchanges:
                for exchange in self.data_output_exchanges:
                    try:
                        exchange.close()
                    except:
                        pass
        finally:
            self.data_output_exchanges = None

    def process_data_messsage(self, message, ack, nack):
        try:
            logging.info("Process message")
            fields = message_protocol.internal.deserialize(message)
        except:
            logging.error("Discarding malformed message")
            ack()
            return

        if len(fields) == MESSAGE_FIELDS:
            action = self._process_data
        elif len(fields) == EOF_FIELDS:
            action = self._process_eof
        elif len(fields) == FLUSH_TOKEN_FIELDS and isinstance(fields[1], list):
            action = self._process_flush_token
        else:
            logging.error("Discarding malformed message")
            ack()
            return

        try:
            action(*fields)
            ack()
        except:
            logging.error("Couldn't process message")
            nack()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data: {client_id},{fruit},{amount}")
        amount_by_fruit = self._clients_amounts.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF message: {client_id}")
        self._flush_and_forward(client_id, [])

    def _process_flush_token(self, client_id, seen):
        logging.info(f"Received flush token: {client_id},{seen}")
        self._flush_and_forward(client_id, seen)

    def _flush_and_forward(self, client_id, seen):
        if ID not in seen:
            self._flush_client(client_id)
            seen = seen + [ID]

        if len(seen) < SUM_AMOUNT:
            self.input_queue.send(
                message_protocol.internal.serialize([client_id, seen])
            )

    def _flush_client(self, client_id):
        logging.info(f"Flushing client: {client_id}")
        client_amounts = self._clients_amounts.pop(client_id, {})

        for final_fruit_item in client_amounts.values():
            target = aggregation_target(final_fruit_item.fruit)
            data_output_exchange = self.data_output_exchanges[target]
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize([client_id])
            )

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            self.close()

    def shutdown(self, *args):
        if self.input_queue:
            self.input_queue.stop_consuming()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGINT, sum_filter.shutdown)
    signal.signal(signal.SIGTERM, sum_filter.shutdown)
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
