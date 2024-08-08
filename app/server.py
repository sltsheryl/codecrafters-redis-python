import asyncio
from app.parser import RedisParser
from app.key_manager import KeyManager


class RedisServer:
    def __init__(
        self,
        host="localhost",
        port=6379,
        role="master",
        master_host=None,
        master_port=None,
    ):
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
        self.master_reader = None
        self.master_writer = None

    async def start(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)

        if self.role == "slave":
            await self.handshake_with_master()

        async with server:
            await server.serve_forever()

    async def handshake_with_master(self):
        self.master_reader, self.master_writer = await asyncio.open_connection(
            self.master_host, self.master_port
        )
        # STEP 1. Ping master
        self.master_writer.write("*1\r\n$4\r\nPING\r\n".encode())
        await self.master_writer.drain()
        data = await self.master_reader.read(1024)
        if data.decode() != "+PONG\r\n":
            raise Exception("Failed to ping master")

        # STEP 2. Send config of slave
        # REPLCONF listening-port <PORT>
        self.master_writer.write(
            f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(self.port))}\r\n{str(self.port)}\r\n".encode()
        )
        await self.master_writer.drain()
        data = await self.master_reader.read(1024)
        if data.decode() != "+OK\r\n":
            raise Exception("Failed to send REPLCONF")
        # REPLCONF capa psync2
        self.master_writer.write(
            "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode()
        )
        await self.master_writer.drain()
        data = await self.master_reader.read(1024)
        if data.decode() != "+OK\r\n":
            raise Exception("Failed to send REPLCONF")

        # STEP 3. Psync with master
        # PSYNC <REPLID> <OFFSET>
        self.master_writer.write(
            "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode()
        )
        await self.master_writer.drain()

        response = await self.master_reader.readuntil(b"\r\n")
        if not response.startswith(b"+FULLRESYNC"):
            raise Exception("Failed to receive FULLRESYNC")

        rdb_size_bulk = await self.master_reader.readuntil(b"\r\n")
        if not rdb_size_bulk.startswith(b"$"):
            raise Exception(f"Expected bulk string for RDB size, got: {rdb_size_bulk}")

        rdb_size = int(
            rdb_size_bulk[1:-2]
        )  # remove '$' and '\r\n', then convert to int

        # actual RDB data
        rdb_data = await self.master_reader.readexactly(rdb_size)  # noqa: F841

        print(f"Received RDB file of size {rdb_size} bytes")

        asyncio.create_task(self.process_propagated_commands())

    async def process_propagated_commands(self):
        while True:
            try:
                command = await self.master_reader.readuntil(b"\r\n")
                if command.startswith(b"*"):  # RESP array
                    num_elements = int(command[1:])
                    full_command = [command]
                    for _ in range(2 * num_elements):
                        element = await self.master_reader.readuntil(b"\r\n")
                        full_command.append(element)

                    full_command_str = b"".join(full_command).decode()
                    if full_command_str.startswith(
                        "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
                    ):
                        response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
                        self.master_writer.write(response.encode())
                        await self.master_writer.drain()

                    else:
                        # parse the command without responding (as master propagates to replicas)
                        self.parser.parse(full_command_str, respond=False)
                else:
                    print(f"Unexpected data from master: {command}")
            except asyncio.IncompleteReadError:
                print("Connection to master closed")
                break
            except Exception as e:
                print(f"Error processing propagated command: {e}")

    async def sync_with_master(self):
        reader, writer = await asyncio.open_connection(
            self.master_host, self.master_port
        )
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
                if self.role == "master" and command[2].upper() in ["SET", "DEL"]:
                    for replica_writer in self.replication_connections:
                        replica_writer.write(data)
                        await replica_writer.drain()
        writer.close()

    def get_info(self):
        return {
            "role": self.role,
            "master_replid": self.replication_id,
            "master_repl_offset": self.offset,
        }

    def set_replica(self, master_host, master_port):
        self.replicas.append((master_host, master_port))
        return "+OK\r\n"
