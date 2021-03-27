import asyncio
import re
import socket

from .messages import PingMessage, EchoMessage, SetMessage, GetMessage

GLOBAL_KEYS = {}
KEY_LOCK = asyncio.Lock()

async def parse_message(message):
    if PingMessage.detect(message):
        return PingMessage(message).response()
    elif EchoMessage.detect(message):
        return EchoMessage(message).response()
    elif SetMessage.detect(message):
        return await SetMessage(message).response(KEY_LOCK, GLOBAL_KEYS)
    elif GetMessage.detect(message):
        return await GetMessage(message).response(KEY_LOCK, GLOBAL_KEYS)
    raise ValueError(f"Unknown message type for: {message}")

async def connected_callback(reader, writer):
    while True:
        data = await reader.read(10000)
        print(f"Received data: {data}")
        message = data.decode('utf-8')
        print(f"Message: {message.encode('utf-8')}")
        try:
            response = await parse_message(message)
        except ValueError as e:
            print(f"Error parsing message: {e}")
        else:
            print(f"Sending response: {bytes(response)}")
            writer.write(bytes(response))
        await writer.drain()
        if reader.at_eof():
            break

async def main():
    print("Starting server...")
    server = await asyncio.start_server(connected_callback, host="127.0.0.1", port=6379, reuse_port=True)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
