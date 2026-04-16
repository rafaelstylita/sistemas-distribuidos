import sys, time
from node import ClusterNode

if __name__ == "__main__":
    node_id = int(sys.argv[1])
    port = 5000
    peer_info = [{'id': i, 'host': f'node{i}', 'port': port} for i in range(5)]
    my_host = f'node{node_id}'
    
    node = ClusterNode(node_id, my_host, port, peer_info)
    while True:
        time.sleep(1)