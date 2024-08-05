
class RedisParser:
    def __init__(self, key_manager):
        self.key_manager = key_manager

    def parse(self, command):
        # command input is in the form of $<length>\r\n<data>\r\n
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
            return self.key_manager.set_key(key, value)
        elif commandWord == "GET":
            if len(lines) < 5:
                return "-ERR invalid command\r\n"
            key = lines[4]
            return self.key_manager.get_key(key)
        else:
            return "-ERR unknown command\r\n"
