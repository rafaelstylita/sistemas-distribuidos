import socket, json, time, sys, random, os

def run_client():
    # Pega configurações das variáveis de ambiente do Docker
    host = os.getenv('NODE_HOST', 'localhost')
    port = int(os.getenv('NODE_PORT', 5000))
    client_id = os.getenv('CLIENT_ID', 'UnkClient')
    
    # REQUISITO: Cada cliente pede entre 10 e 50 acessos
    num_requests = random.randint(1, 3)
    
    print(f"--- Cliente {client_id} iniciando: pedirá {num_requests} acessos ao Nó {host} ---")

    for i in range(num_requests):
        try:
            # Conecta apenas ao seu elemento específico do Cluster Sync
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(30)
                s.connect((host, port))
                
                # Envia ID único e timestamp
                req = {
                    "type": "client_request", 
                    "client_id": client_id, 
                    "timestamp": time.time()
                }
                s.sendall((json.dumps(req) + '\n').encode('utf-8'))
                
                data = s.recv(4096).decode('utf-8')
                if data and '"type": "committed"' in data:
                    print(f"[{client_id}] Acesso {i+1}/{num_requests} COMMITTED.")
                
                # REQUISITO: Sleep de 1 a 5 segundos após COMMITTED
                wait = random.uniform(1, 5)
                time.sleep(wait)
        except Exception as e:
            print(f"[{client_id}] Erro: {e}. Tentando novamente em 2s...")
            time.sleep(2)

    print(f"--- Cliente {client_id} finalizou todos os {num_requests} pedidos. ---")

if __name__ == "__main__":
    # Pequeno delay para garantir que os nós subiram
    time.sleep(5)
    run_client()