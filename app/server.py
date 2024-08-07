import asyncio
from app.parser import RedisParser
from app.key_manager import KeyManager

class RedisServer:
    def __init__(self, host='localhost', port=6379, role='master', master_host=None, master_port=None):
        self.host = host
        self.port = port
        self.role = role
        self.master_host = master_host
        self.master_port = master_port
        self.key_manager = KeyManager()
        self.parser = RedisParser(self.key_manager, self)
        self.replicas = []
        self.replication_connections = [] 
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
            elif command[0] == "*3" and command[2].upper() == "PSYNC":
                # master cannot perform incremental replication with the replica, and will thus start a "full" resynchronization
                writer.write(f"+FULLRESYNC {self.replication_id} 0\r\n".encode())
                # send empty rdb file
                # retrieved from https://github.com/codecrafters-io/redis-tester/blob/main/internal/assets/empty_rdb_hex.md
                empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
                empty_rdb_bytes = bytes.fromhex(empty_rdb_hex)
                # $<length_of_file>\r\n<contents_of_file>
                writer.write(f"${len(empty_rdb_bytes)}\r\n".encode())
                writer.write(empty_rdb_bytes)
                await writer.drain()
                # add the writer to the replication connections
                # so it's the same as the replica connections
                self.replication_connections.append(writer)
            else:
                response = self.parser.parse(data.decode())
                writer.write(response.encode())
                await writer.drain()
                # update replicas for write commands
                if self.role == 'master' and command[2].upper() in ["SET", "DEL"]:
                    for replica_writer in self.replication_connections:
                        replica_writer.write(data)
                        await replica_writer.drain()
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
