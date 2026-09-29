import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self._client_id = uuid.uuid4().int
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self._client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self._client_id])

    def deserialize_result_message(self, message):
        [client_id, fields] = message_protocol.internal.deserialize(message)
        if self._client_id == client_id:
            return fields
