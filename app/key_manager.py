class KeyManager:
    def __init__(self):
        self.key_store = {}

    def set_key(self, key, value):
        self.key_store[key] = value
        return f"+OK\r\n"

    def get_key(self, key):
        if key in self.key_store:
            return f"${len(self.key_store[key])}\r\n{self.key_store[key]}\r\n"
        else:
            # null bulk string
            return "$-1\r\n"

