import re

class SimpleString:
    @staticmethod
    def detect(msg):
        match = re.match(r'^\+(?P<message>[^\r\n]+)\r\n(?P<rest>.*)', msg)
        if not match:
            return False
        return match.groupdict()

    @staticmethod
    def parse(msg):
        matches = SimpleString.detect(msg)
        if not matches:
            raise ValueError(f"Not a SimpleString: {msg}")
        return SimpleString(matches['message']), matches['rest']

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"+{self.message}\r\n"

    def __bytes__(self):
        return str(self).encode('utf-8')

class BulkString:
    @staticmethod
    def detect(msg):
        print(f"BulkString.detect msg: {msg.encode('utf-8')}")
        lines = msg.split("\r\n")

        header_match = re.match(r'^\$(?P<length>[\-0-9]+)', lines[0])
        if not header_match:
            return False

        rest = "\r\n".join(lines[2:])
        return {'string': lines[1], 'rest': rest}

    @staticmethod
    def parse(msg):
        matches = BulkString.detect(msg)
        if not matches:
            raise ValueError(f"Not a BulkString: {msg}")
        print(f"Matches: {matches}")
        return BulkString(matches['string']), matches['rest']

    def __init__(self, message):
        self.message = message

    def __str__(self):
        if self.message is None:
            return f"$-1\r\n"

        return f"${len(self.message)}\r\n{self.message}\r\n"

    def __bytes__(self):
        return str(self).encode('utf-8')

class Error:
    @staticmethod
    def detect(msg):
        match = re.match(r'^\-(?P<message>[^\r\n]+)\r\n(?P<rest>.*)', msg)
        if not match:
            return False
        return match.groupdict()

    @staticmethod
    def parse(msg):
        matches = Error.detect(msg)
        if not matches:
            raise ValueError(f"Not an Error: {msg}")
        return Error(matches['message']), matches['rest']

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"-{self.message}\r\n"

    def __bytes__(self):
        return str(self).encode('utf-8')

class Integer:
    @staticmethod
    def detect(msg):
        match = re.match(r'^\:(?P<message>[^0-9]+)\r\n(?P<rest>.*)', msg)
        if not match:
            return False
        return match.groupdict()

    @staticmethod
    def parse(msg):
        matches = Integer.detect(msg)
        if not matches:
            raise ValueError(f"Not a Integer: {msg}")
        return Integer(matches['message']), matches['rest']

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f":{self.message}\r\n"

    def __bytes__(self):
        return str(self).encode('utf-8')

class Array:
    @staticmethod
    def detect(msg):
        lines = msg.split("\r\n")
        if len(lines) < 1:
            return False

        header = lines[0]
        if not header.startswith("*"):
            return False

        try:
            num_entries = int(header[1:])
        except ValueError:
            return False
        return True

    @staticmethod
    def parse(msg):
        if not Array.detect(msg):
            raise ValueError(f"Not a Array: {msg}")
        lines = msg.split("\r\n")
        messages = []
        remaining_string = "\r\n".join(lines[1:])
        while remaining_string:
            print(f"Remaining string: {remaining_string.encode('utf-8')}")
            if SimpleString.detect(remaining_string):
                parsed, remaining_string = SimpleString.parse(remaining_string)
                messages.append(parsed)
            elif BulkString.detect(remaining_string):
                parsed, remaining_string = BulkString.parse(remaining_string)
                messages.append(parsed)
            elif Error.detect(remaining_string):
                parsed, remaining_string = Error.parse(remaining_string)
                messages.append(parsed)
            elif Integer.detect(remaining_string):
                parsed, remaining_string = Integer.parse(remaining_string)
                messages.append(parsed)
            else:
                raise ValueError(f"Unknown message type in array: {remaining_string}")
        return Array(messages), ""

    def __init__(self, messages):
        self.messages = messages

    def __str__(self):
        encoded_messages = [str(message) for message in self.messages]
        return f"*{len(self.messages)}\r\n{''.join(encoded_messages)}"

    def __bytes__(self):
        return str(self).encode('utf-8')