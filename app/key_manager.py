import time
import threading

# threading used to ensure main is not blocked
class KeyManager:
    def __init__(self):
        self.key_store = {}
        self.lock = threading.Lock()
        self.cleanup_interval = 10
        self.cleanup()

    def set_key(self, key, value, expiry=None):
        with self.lock:
            if expiry:
                expiry_time = time.time() + expiry/1000
            else:
                expiry_time = None
            self.key_store[key] = (value, expiry_time)
            return "+OK\r\n"

    def cleanup(self):
        threading.Timer(self.cleanup_interval, self.cleanup).start()
        with self.lock:
            for key, (value, expiry_time) in list(self.key_store.items()):
                if expiry_time and expiry_time < time.time():
                    del self.key_store[key]
    
    def get_key(self, key):
        with self.lock:
            if key in self.key_store:
                value, expiry_time = self.key_store[key]
                if expiry_time and expiry_time < time.time():
                    del self.key_store[key]
                    return "$-1\r\n"
                return f"${len(value)}\r\n{value}\r\n"
            else:
                return "$-1\r\n"

