import asyncio
from app.parser import RedisParser
from app.key_manager import KeyManager

class RedisServer:
    def __init__(self, host = 'localhost', port = 6379, role='master', master_host=None, master_port=None):
        self.host = host
        self.port = port
        self.role = role
        self.master_host = master_host
        self.master_port = master_port
        self.key_manager = KeyManager()
        self.parser = RedisParser(self.key_manager, self)
        self.replicas = []
        self.replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.offset = 0

    async def start(self):
        if self.role == 'slave':
            # ping master
            await self.ping_master()
            # sync with master
            asyncio.create_task(self.sync_with_master())
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        async with server:
            await server.serve_forever()
        
    
    async def ping_master(self):
        reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
        # PING command should be sent as a RESP Array
        writer.write("*1\r\n$4\r\nPING\r\n".encode())
        await writer.drain()
        data = await reader.read(1024)
        if data.decode() != "+PONG\r\n":
            raise Exception("Failed to ping master")
        writer.close()

    async def sync_with_master(self):
        # replicate data from master
        reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
        while True:
            data = await reader.read(1024)
            if not data:
                break
            self.parser.parse(data.decode())
        writer.close()

    # use eventloop
    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(1024)
            if not data:
                break
            response = self.parser.parse(data.decode())
            writer.write(response.encode())
            await writer.drain()
            # update replicas
            if self.role == 'master':
                for replica in self.replicas:
                    replica.write(response.encode())
                    await replica.drain()
        writer.close()

    def get_info(self):
        return {
            'role': self.role,
            'master_replid': self.replication_id,
            'master_repl_offset': self.offset,
        }

    def set_replica(self, master_host, master_port):
        self.replicas.append((master_host, master_port))
        return "+OK\r\n"
