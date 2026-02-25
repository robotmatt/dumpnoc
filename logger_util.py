import sys
import threading
from collections import deque
import time

class LogBuffer:
    def __init__(self, maxlen=500):
        self.buffer = deque(maxlen=maxlen)
        self.lock = threading.Lock()

    def add(self, message):
        if not message.strip():
            return
        # Split by newlines in case multiple lines come at once
        lines = message.split('\n')
        with self.lock:
            for line in lines:
                if line.strip():
                    timestamp = time.strftime("%H:%M:%S")
                    self.buffer.append(f"[{timestamp}] {line.strip()}")

    def get_all(self):
        with self.lock:
            return list(self.buffer)

    def get_last(self, n=50):
        with self.lock:
            return list(self.buffer)[-n:]

# Global singleton
log_buffer = LogBuffer()

class RedirectedOutput:
    def __init__(self, original_stream, buffer):
        self.original_stream = original_stream
        self.buffer = buffer

    def write(self, message):
        self.original_stream.write(message)
        self.buffer.add(message)

    def flush(self):
        self.original_stream.flush()

    def isatty(self):
        return self.original_stream.isatty()

    @property
    def encoding(self):
        return getattr(self.original_stream, 'encoding', 'utf-8')

def init_logging():
    """
    Initializes the redirection of stdout and stderr to the global log buffer.
    Can be called multiple times safely.
    """
    if not hasattr(init_logging, "_initialized"):
        sys.stdout = RedirectedOutput(sys.stdout, log_buffer)
        sys.stderr = RedirectedOutput(sys.stderr, log_buffer)
        init_logging._initialized = True
        print("[System] Terminal logging redirection initialized.")
