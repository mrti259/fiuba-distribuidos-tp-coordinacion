import os
import logging
import threading
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

MESSAGE_FIELDS = 3


class SumFilter:
    def __init__(self):
        self._clients_amounts_lock = threading.Lock()
        self._clients_amounts = {}
        self._control_thread = None
        try:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )
            self.data_output_exchanges = []
            for i in range(AGGREGATION_AMOUNT):
                data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
                )
                self.data_output_exchanges.append(data_output_exchange)

            self.control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_PREFIX]
            )
            self.control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_PREFIX]
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
            if self.data_output_exchanges:
                for exchange in self.data_output_exchanges:
                    try:
                        exchange.close()
                    except:
                        pass
        finally:
            self.data_output_exchanges = None

        try:
            if self.control_output_exchange:
                self.control_output_exchange.close()
        finally:
            self.control_output_exchange = None

        try:
            if self.control_input_exchange:
                if self._control_thread:
                    self.control_input_exchange.stop_consuming()
                    self._control_thread.join(timeout=5)
                    self._control_thread = None
                self.control_input_exchange.close()
        finally:
            self.control_input_exchange = None

    def process_data_messsage(self, message, ack, nack):
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

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data: {client_id},{fruit},{amount}")
        with self._clients_amounts_lock:
            amount_by_fruit = self._clients_amounts.setdefault(client_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting EOF message: {client_id}")
        self.control_output_exchange.send(
            message_protocol.internal.serialize([client_id])
        )

    def process_control_message(self, message, ack, nack):
        try:
            logging.info("Process control message")
            fields = message_protocol.internal.deserialize(message)
            self._flush_client(*fields)
            ack()
        except:
            logging.error("Couldn't process control message")
            nack()

    def _flush_client(self, client_id):
        logging.info(f"Flushing client: {client_id}")
        with self._clients_amounts_lock:
            client_amounts = self._clients_amounts.pop(client_id, {})

        target = client_id % AGGREGATION_AMOUNT
        data_output_exchange = self.data_output_exchanges[target]

        for final_fruit_item in client_amounts.values():
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def start(self):
        try:
            self._control_thread = threading.Thread(
                target=self.control_input_exchange.start_consuming,
                args=(self.process_control_message,),
                daemon=True,
            )
            self._control_thread.start()
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
