import socket
import threading
import json
import time
import random
from queue import Queue
from typing import Dict, List

class ClusterNode:
    def __init__(self, node_id: int, host: str, port: int, peer_info: List[Dict]):
        self.node_id = node_id
        self.host = host
        self.port = port 
        self.peers = peer_info
        
        # Variáveis do Algoritmo Ricart-Agrawala
        self.lamport_clock = 0
        self.state = "RELEASED"  # Estados: RELEASED, WANTED, HELD
        self.request_clock = 0
        self.received_ok_count = 0
        self.deferred_replies = [] # Esta é a nossa "Fila de Espera"
        
        self.client_queue = Queue()
        self.lock = threading.Lock()
        self.peer_sockets: Dict[int, socket.socket] = {}
        self.running = True

        # Configuração do Servidor
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen(20)

        # Threads de suporte
        threading.Thread(target=self.listener, daemon=True).start()
        threading.Thread(target=self.connection_manager, daemon=True).start()
        threading.Thread(target=self.process_client_queue, daemon=True).start()

    def log(self, msg):
        print(f"[Nó {self.node_id}] {msg}")

    def connection_manager(self):
        """Mantém conexão ativa com todos os outros nós do cluster"""
        for peer in self.peers:
            if peer['id'] == self.node_id: continue
            while self.running and peer['id'] not in self.peer_sockets:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((peer['host'], peer['port']))
                    self.peer_sockets[peer['id']] = s
                    self.log(f"Conectado ao Peer {peer['id']}")
                except:
                    time.sleep(1)

    def listener(self):
        """Escuta novas conexões (de outros nós ou de clientes)"""
        while self.running:
            try:
                conn, _ = self.server_socket.accept()
                threading.Thread(target=self.handle_connection, args=(conn,), daemon=True).start()
            except: break

    def handle_connection(self, conn: socket.socket):
        with conn:
            while self.running:
                try:
                    data = conn.recv(4096).decode('utf-8')
                    if not data: break
                    for line in data.splitlines():
                        if line.strip():
                            self.process_message(json.loads(line), conn)
                except: break

    def process_message(self, msg: dict, conn: socket.socket):
        m_type = msg.get('type')
        
        if m_type == "client_request":
            self.client_queue.put((msg['client_id'], conn))

        elif m_type == "request":
            with self.lock:
                remote_clock = msg['lamport']
                remote_id = msg['node_id']
                
                # Sincronização de Lamport: max(local, remoto) + 1
                self.lamport_clock = max(self.lamport_clock, remote_clock) + 1
                
                # Critério de Prioridade: Ticket menor vence. Se empate, ID menor vence.
                is_higher_priority = (remote_clock < self.request_clock) or \
                                     (remote_clock == self.request_clock and remote_id < self.node_id)
                
                # Se eu estou na CS ou se eu quero entrar e tenho prioridade, eu ADIO o OK
                if self.state == "HELD" or (self.state == "WANTED" and not is_higher_priority):
                    if remote_id not in self.deferred_replies:
                        self.deferred_replies.append(remote_id)
                    self.log(f"--- FILA DE ESPERA: {self.deferred_replies} (Adiei OK para Nó {remote_id}) ---")
                else:
                    # Caso contrário, libero o OK imediatamente
                    self.send_msg(remote_id, {"type": "ok", "from": self.node_id})

        elif m_type == "ok":
            with self.lock:
                self.received_ok_count += 1

    def send_msg(self, peer_id: int, msg: dict):
        if peer_id in self.peer_sockets:
            try:
                data = (json.dumps(msg) + '\n').encode('utf-8')
                self.peer_sockets[peer_id].sendall(data)
            except:
                if peer_id in self.peer_sockets: del self.peer_sockets[peer_id]

    def process_client_queue(self):
        """Lógica Principal de Exclusão Mútua"""
        while self.running:
            client_data = self.client_queue.get()
            client_id, conn = client_data
            
            with self.lock:
                self.state = "WANTED"
                self.lamport_clock += 1
                self.request_clock = self.lamport_clock # Meu "Ticket" de entrada
                self.received_ok_count = 0
                current_peers = list(self.peer_sockets.keys())
            
            self.log(f"Solicitando CS para {client_id} | TICKET LAMPORT: {self.request_clock}")
            
            # Envia solicitação para todos os pares
            for pid in current_peers:
                self.send_msg(pid, {"type": "request", "lamport": self.request_clock, "node_id": self.node_id})

            # Espera até receber OK de todos (N-1)
            while True:
                with self.lock:
                    if self.received_ok_count >= len(current_peers):
                        self.state = "HELD"
                        break
                time.sleep(0.05)

            # --- INÍCIO DA SEÇÃO CRÍTICA (RECURSO R) ---
            self.log(f"!!! ACESSANDO RECURSO R para {client_id} (Ticket: {self.request_clock}) !!!")
            
            # REQUISITO: Sleep de 0.2 a 1 segundo na CS
            time.sleep(random.uniform(0.5, 1.0)) 
            
            try:
                conn.sendall(b'{"type": "committed"}\n')
            except: pass
            # --- FIM DA SEÇÃO CRÍTICA ---

            with self.lock:
                self.state = "RELEASED"
                to_reply = list(self.deferred_replies)
                self.deferred_replies = []
                self.request_clock = 0 # Reseta ticket atual
            
            # Libera quem estava na espera
            if to_reply:
                self.log(f"--- SAINDO: Enviando OKs adiados para {to_reply} ---")
            
            for pid in to_reply:
                self.send_msg(pid, {"type": "ok", "from": self.node_id})
            
            self.log(f"Recurso R liberado. {client_id} finalizado.")