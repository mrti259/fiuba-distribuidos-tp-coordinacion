from common import message_protocol

last_client_id = 0

class MessageHandler:

    def __init__(self):
        global last_client_id
        self._client_id = last_client_id + 1
        last_client_id = self._client_id
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self._client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self._client_id])

    def deserialize_result_message(self, message):
        [client_id, fields] = message_protocol.internal.deserialize(message)
        if self._client_id == client_id:
            return fields
