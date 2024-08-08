class RedisParser:
    def __init__(self, key_manager, server):
        self.key_manager = key_manager
        self.server = server

    def parse(self, command, respond=True):
        # command input is in the form of $<length>\r\n<data>\r\n
        result = self.execute_command(command)
        if respond:
            return result
        return None

    def execute_command(self, command):
        lines = command.split("\r\n")
        if len(lines) == 0:
            return "-ERR empty command\r\n"

        if len(lines) < 3:
            return "-ERR invalid command\r\n"

        commandWord = lines[2].upper()

        if commandWord == "PING":
            return "+PONG\r\n"

        elif commandWord == "ECHO":
            if len(lines) < 5:
                return "-ERR invalid command\r\n"
            message = lines[4]
            return f"${len(message)}\r\n{message}\r\n"

        elif commandWord == "SET":
            if len(lines) < 7:
                return "-ERR invalid command\r\n"
            key = lines[4]
            value = lines[6]
            # px expiry is set
            expiry = None
            if len(lines) > 8 and lines[8].upper() == "PX":
                if len(lines) < 11:
                    return "-ERR invalid command\r\n"
                try:
                    expiry = int(lines[10])
                except ValueError:
                    return "-ERR invalid expiry\r\n"
            return self.key_manager.set_key(key, value, expiry)

        elif commandWord == "GET":
            if len(lines) < 5:
                return "-ERR invalid command\r\n"
            key = lines[4]
            return self.key_manager.get_key(key)

        elif commandWord == "REPLICAOF":
            if len(lines) < 5:
                return "-ERR invalid command\r\n"
            master_host = lines[4]
            master_port = int(lines[6])
            return self.server.set_replica(master_host, master_port)

        elif commandWord == "INFO":
            info = self.server.get_info()
            info_str = "\r\n".join([f"{key}:{value}" for key, value in info.items()])
            return f"${len(info_str)}\r\n{info_str}\r\n"

        elif commandWord == "REPLCONF":
            if len(lines) < 5:
                return "-ERR invalid command\r\n"
            replConfigCommand = lines[4].upper()
            if replConfigCommand == "GETACK":
                return f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(self.server.offset))}\r\n{self.server.offset}\r\n"
            else:
                return "-ERR unknown REPLCONF command\r\n"

        else:
            return "-ERR unknown command\r\n"
