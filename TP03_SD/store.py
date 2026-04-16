import socket
import threading
import json
import sys
import time
import os

class StoreNode:
    def __init__(self, store_id, port=6000):
        self.store_id = store_id
        self.port = port
        self.running = True

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen(20)

        print(f"[STORE {self.store_id}] Iniciado na porta {self.port}")

        threading.Thread(target=self.listener, daemon=True).start()

    def listener(self):
        while self.running:
            try:
                conn, _ = self.server_socket.accept()
                threading.Thread(target=self.handle_connection, args=(conn,), daemon=True).start()
            except:
                break

    def handle_connection(self, conn):
        with conn:
            try:
                data = conn.recv(4096).decode('utf-8')
                if not data:
                    return

                for line in data.splitlines():
                    if line.strip():
                        msg = json.loads(line)
                        self.process_message(msg, conn)
            except:
                pass

    def process_message(self, msg, conn):
        if msg.get("type") == "write":
            entry = (
                f"STORE {self.store_id} | "
                f"Node {msg['from_node']} | "
                f"Client {msg['client_id']} | "
                f"Lamport {msg['lamport']} | "
                f"Time {time.time()}\n"
            )

            filename = f"store_{self.store_id}_data.txt"

            with open(filename, "a") as f:
                f.write(entry)

            print(f"[STORE {self.store_id}] Escrita realizada para {msg['client_id']}")

            ack = {"type": "ack"}
            conn.sendall((json.dumps(ack) + "\n").encode('utf-8'))


if __name__ == "__main__":
    store_id = int(sys.argv[1])
    StoreNode(store_id)
    while True:
        time.sleep(1)