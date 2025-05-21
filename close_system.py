import Pyro5.api
import subprocess as sub
import signal as sig
import sys
import os


def fechar_processos_por_porta(portas):
    try:
        if sys.platform == 'win32':
            for porta in portas:
                result = sub.run(['netstat', '-ano'], capture_output=True, text=True)
                for line in result.stdout.split('\n'):
                    if f':{porta}' in line and 'LISTENING' in line:
                        pid = line.split()[-1]
                        os.kill(int(pid), sig.SIGTERM)
                        print(f"Encerrado processo {pid} na porta {porta}")
        else:
            for porta in portas:
                result = sub.run(['lsof', '-i', f':{porta}'], capture_output=True, text=True)
                for line in result.stdout.split('\n')[1:]:
                    if line.strip():
                        pid = line.split()[1]
                        os.kill(int(pid), sig.SIGTERM)
                        print(f"Encerrado processo {pid} na porta {porta}")
    except Exception as e:
        print(f"Erro ao encerrar processos: {e}")


def fechar_nameserver():
    try:
        ns = Pyro5.api.locate_ns()
        ns._pyroTimeout = 2
        ns.shutdown()
        print("NameServer encerrado com sucesso")
    except:
        print("NameServer não encontrado ou já encerrado")


def fechar_todos_peers():
    try:
        ns = Pyro5.api.locate_ns()
        for name, uri in ns.list().items():
            if name.startswith(('Peer_', 'Tracker_Epoca_')):
                try:
                    peer = Pyro5.api.Proxy(uri)
                    peer._pyroTimeout = 2
                    peer.shutdown()
                    print(f"Peer {name} encerrado")
                except:
                    print(f"Falha ao encerrar {name}")
    except:
        print("Não foi possível acessar o NameServer")


def run():
    portas_peers = list(range(9000, 9005))
    
    print("Encerrando todos os componentes do sistema P2P...")

    fechar_todos_peers()
    fechar_processos_por_porta(portas_peers)
    fechar_nameserver()
    
    # 4. Tenta encerrar o processo pyro5-ns
    try:
        if sys.platform == 'win32':
            sub.run(['taskkill', '/f', '/im', 'python.exe'], shell=True)
            sub.run(['taskkill', '/f', '/im', 'pyro5-ns.exe'], shell=True)
        else:
            sub.run(['pkill', '-f', 'pyro5-ns'], shell=True)
            sub.run(['pkill', '-f', 'peer.py'], shell=True)
    except:
        pass
    
    print("Sistema P2P completamente encerrado")

if __name__ == "__main__":
    run()