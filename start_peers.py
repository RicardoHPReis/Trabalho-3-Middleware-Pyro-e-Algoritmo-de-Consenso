import close_system as close
import subprocess as sub
import threading as th
import datetime as dt
import socket as s
import time as t
import os


def iniciar_nameserver():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("Iniciando Pyro NameServer...")
    sub.Popen("pyro5-ns", shell=True, creationflags=sub.CREATE_NEW_CONSOLE)
    t.sleep(3)
    print("Pyro NameServer iniciado")


def log_server():
    sock = s.socket(s.AF_INET, s.SOCK_DGRAM)
    sock.bind(('localhost', 9999))
    print("Servidor de logs iniciado (localhost:9999)\n")
    print("="*50 + " LOGS DO SISTEMA " + "="*50)
    
    while True:
        try:
            data, addr = sock.recvfrom(1024)
            timestamp = dt.datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f"[{timestamp}] {data.decode()}")
        except Exception as e:
            print(f"Erro no servidor de logs: {e}")
            break


def iniciar_peer(peer_id, port, is_tracker=False):
    dir_name = f"peer_{peer_id}_files"
    os.makedirs(dir_name, exist_ok=True)
    
    comando = (f"python peer.py --id {peer_id} --port {port} --dir {dir_name}")
    
    if is_tracker:
        comando += " --tracker"
        print(f"Iniciando tracker (Peer {peer_id}) na porta {port}")
    else:
        print(f"Iniciando peer {peer_id} na porta {port}")
    
    sub.Popen(["cmd", "/c", "start", "cmd", "/k", comando], shell=True)
    t.sleep(1)


if __name__ == "__main__":
    try:
        log = th.Thread(target=log_server, daemon=True)
        log.start()
        iniciar_nameserver()
        iniciar_peer(0, 9000, is_tracker=True)
        
        for i in range(1, 5):
            iniciar_peer(i, 9000 + i)
        
        while True:
            pass
    except KeyboardInterrupt:
        close.run()
    except Exception as e:
        print(f"Erro fatal: {e}")
        close.run()