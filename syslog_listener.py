# syslog_listener.py
import asyncio
import socketserver
import json
from datetime import datetime
import requests

# Configuration
SYSLOG_UDP_PORT = 5140  # Using 5140 to avoid root privileges for 514
BACKEND_INGEST_URL = "http://localhost:8000/ingest"

class SyslogUDPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data, socket = self.request
        message = data.strip().decode('utf-8', errors='ignore')
        print(f"Received syslog from {self.client_address[0]}: {message}")
        
        # Parse and forward to our backend ingest API
        log_entry = {
            "source": "syslog",
            "source_ip": self.client_address[0],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "raw_message": message,
            "severity": "Unknown"  # Basic parsing - can be enhanced
        }
        
        try:
            # Forward to our backend for queuing and processing
            response = requests.post(BACKEND_INGEST_URL, json=log_entry)
            print(f"Forwarded to backend: {response.status_code}")
        except Exception as e:
            print(f"Error forwarding log: {e}")

if __name__ == "__main__":
    print(f"Starting Syslog UDP server on port {SYSLOG_UDP_PORT}")
    with socketserver.UDPServer(('0.0.0.0', SYSLOG_UDP_PORT), SyslogUDPHandler) as server:
        server.serve_forever()
