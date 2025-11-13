# log_agent.py
import time
import os
import json
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuration
LOG_FILE_PATH = "/var/log/auth.log"  # Example: Linux auth logs
BACKEND_INGEST_URL = "http://your-backend-ip:8000/ingest"

class LogFileHandler(FileSystemEventHandler):
    def __init__(self, filename):
        self.filename = filename
        self._current_position = 0
        # Start from the end of the file if it exists
        if os.path.exists(filename):
            self._current_position = os.path.getsize(filename)
    
    def on_modified(self, event):
        if not event.is_directory and event.src_path == self.filename:
            self.tail_new_lines()
    
    def tail_new_lines(self):
        try:
            with open(self.filename, 'r') as file:
                file.seek(self._current_position)
                new_lines = file.readlines()
                self._current_position = file.tell()
                
                for line in new_lines:
                    if line.strip():
                        self.process_log_line(line.strip())
        except Exception as e:
            print(f"Error reading log file: {e}")
    
    def process_log_line(self, line):
        log_entry = {
            "source": "file_agent",
            "filename": self.filename,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "raw_message": line,
            "hostname": os.uname().nodename
        }
        
        try:
            response = requests.post(BACKEND_INGEST_URL, json=log_entry)
            print(f"Sent log to backend: {response.status_code}")
        except Exception as e:
            print(f"Error sending log: {e}")

def start_file_tailing():
    event_handler = LogFileHandler(LOG_FILE_PATH)
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(LOG_FILE_PATH), recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_file_tailing()
