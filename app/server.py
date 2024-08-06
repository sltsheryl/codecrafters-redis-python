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
            await self.handshake_with_master()
            asyncio.create_task(self.sync_with_master())
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        async with server:
            await server.serve_forever()
        
    async def handshake_with_master(self):
        reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
        # Step 1. Ping master 
        writer.write("*1\r\n$4\r\nPING\r\n".encode())
        await writer.drain()
        data = await reader.read(1024)
        if data.decode() != "+PONG\r\n":
            raise Exception("Failed to ping master")
        
        # Step 2. Send config of slave
        # REPLCONF listening-port <PORT>
        writer.write(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(self.port))}\r\n{str(self.port)}\r\n".encode())
        await writer.drain()
        data = await reader.read(1024)
        if data.decode() != "+OK\r\n":
            raise Exception("Failed to send REPLCONF")
        # REPLCONF capa psync2
        writer.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode())
        await writer.drain()
        data = await reader.read(1024)
        if data.decode() != "+OK\r\n":
            raise Exception("Failed to send REPLCONF")
        
        # Step 3. Psync with master
        # PSYNC <REPLID> <OFFSET>
        writer.write(f"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())
        await writer.drain()
        data = await reader.read(1024)
        
        
    async def sync_with_master(self):
        reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
        while True:
            data = await reader.read(1024)
            if not data:
                break
            self.parser.parse(data.decode())
        writer.close()

    # master handling replicas
    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(1024)
            if not data:
                break
            command = data.decode().split("\r\n")
            if len(command) < 3:
                writer.write("-ERR invalid command\r\n".encode())
                await writer.drain()
                continue
            if command[0] == "*3" and command[2].upper() == "REPLCONF":
                if command[4].upper() == "LISTENING-PORT":
                    self.port = int(command[6])
                    writer.write("+OK\r\n".encode())
                    await writer.drain()
                    continue
                elif command[4].upper() == "CAPA" and command[6].upper() == "PSYNC2":
                    writer.write("+OK\r\n".encode())
                    await writer.drain()
                    continue
            else:    
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
