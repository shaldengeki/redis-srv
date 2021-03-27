import datetime
import re

from .resp import SimpleString, BulkString, Error, Integer, Array

class PingMessage:
    @staticmethod
    def detect(msg):
        array, leftover = Array.parse(msg)
        message = array.messages[0].message

        match = re.match(r"(PING)( )?(?P<text>.+)?", message, re.IGNORECASE)

        if not match:
            return False

        return match.groupdict("PONG")

    def __init__(self, message):

        matches = self.__class__.detect(message)
        if not matches:
            raise ValueError(f"Message is not a ping: {message}")

        self._argument = matches['text']

    def response(self):
        return SimpleString(self._argument)

class EchoMessage:
    @staticmethod
    def detect(msg):
        array, leftover = Array.parse(msg)

        print(f"Parsed messages: {array.messages}")

        command = array.messages[0].message
        if command.lower() != "echo":
            return False
        return {'command': command, 'text': array.messages[1].message}

    def __init__(self, message):

        matches = self.__class__.detect(message)
        if not matches:
            raise ValueError(f"Message is not an echo: {message}")

        self._argument = matches['text']

    def response(self):
        return BulkString(self._argument)

class SetMessage:
    @staticmethod
    def detect(msg):
        array, leftover = Array.parse(msg)

        print(f"Parsed messages: {array.messages}")

        command = array.messages[0].message
        if command.lower() != "set":
            return False

        results = {
            'command': command,
            'key': array.messages[1].message,
            'value': array.messages[2].message,
            'expiry': -1
        }

        if len(array.messages) > 3 and array.messages[3].message.lower() == "px":
            results['expiry'] = datetime.datetime.utcnow().timestamp() + (float(array.messages[4].message) / 1000.0)

        return results

    def __init__(self, message):
        matches = self.__class__.detect(message)
        if not matches:
            raise ValueError(f"Message is not a set: {message}")

        self._key = matches['key']
        self._value = matches['value']
        self._expiry = matches['expiry']

    async def response(self, lock, keys):
        async with lock:
            keys[self._key] = (self._value, self._expiry)
            print(f"Keys: {keys}")
        return SimpleString("OK")

class GetMessage:
    @staticmethod
    def detect(msg):
        array, leftover = Array.parse(msg)

        print(f"Parsed messages: {array.messages}")

        command = array.messages[0].message
        if command.lower() != "get":
            return False

        return {'command': command, 'key': array.messages[1].message}

    def __init__(self, message):

        matches = self.__class__.detect(message)
        if not matches:
            raise ValueError(f"Message is not a get: {message}")

        self._key = matches['key']

    async def response(self, lock, keys):
        async with lock:
            value = keys.get(self._key)
            if value is not None:
                val, expiry = value
                if expiry > 0 and expiry < datetime.datetime.utcnow().timestamp():
                    del keys[self._key]
                    val = None
                value = val
        return BulkString(value)

