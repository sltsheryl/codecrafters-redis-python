import asyncio
from app.parser import RedisParser

class RedisServer:
    def __init__(self, host = 'localhost', port = 6379):
        self.host = host
        self.port = port
        self.parser = RedisParser()

    # use eventloop
    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(1024)
            if not data:
                break
            response = self.parser.parse(data.decode())
            writer.write(response.encode())
            await writer.drain()
        writer.close()

    async def start(self):
        server = await asyncio.start_server(self.handle_client, 'localhost', 6379)
        async with server:
            await server.serve_forever()
