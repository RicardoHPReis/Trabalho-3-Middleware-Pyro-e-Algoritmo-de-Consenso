import Pyro5.api
import collections as c
import threading as th
import argparse as arg
import random as r
import socket as s
import time as t
import os

@Pyro5.api.expose
class Peer:
    def __init__(self, peer_id, port, files_dir, is_tracker=False):
        self.peer_id = peer_id
        self.PORT = port
        self.FILES_DIR = files_dir
        self.LOG_HOST = 'localhost'
        self.LOG_PORT = 9999
        
        self.is_tracker = is_tracker
        
        self.epoca = 0
        self.tracker_uri = None
        self.election_timeout = None
        self.HEARTBEAT_INTERVALO = r.uniform(2.0, 3.0)
        self.ELEICAO_TIMEOUT = r.uniform(8.0, 10.0)
        self.VERIFICACAO_TRACKER = 10.0
        self.votos_recebidos = set()
        self.peer_votou = -False
        self.peers_conhecidos = []
        self.eleicao_em_curso = False
        self.epoca_registro_arquivos = -1
        self.index_arquivos = c.defaultdict(set)
        
        self.criar_arquivos()
        self.iniciar_servico()
        self.registrar_no_sistema()
        
        self.verificador_tracker_thread = th.Thread(target=self.verificar_tracker_periodicamente, daemon=True)
        self.verificador_tracker_thread.start()


    def _log(self, message):
        try:
            with s.socket(s.AF_INET, s.SOCK_DGRAM) as sock:
                sock.sendto(f"[Peer {self.peer_id}] {message}".encode(), 
                          (self.LOG_HOST, self.LOG_PORT))
        except:
            pass


    def criar_arquivos(self):
        os.makedirs(self.FILES_DIR, exist_ok=True)
        for i in range(1, 3):
            path = os.path.join(self.FILES_DIR, f"file_{self.peer_id}_{i}.txt")
            if not os.path.exists(path):
                with open(path, "w") as f:
                    f.write(f"Conteúdo do peer {self.peer_id}, arquivo {i}\n")


    def iniciar_servico(self):
        self.daemon = Pyro5.api.Daemon(host="localhost", port=self.PORT)
        self.uri = self.daemon.register(self)
        th.Thread(target=self.daemon.requestLoop, daemon=True).start()
        print(f"Peer {self.peer_id} iniciado na porta {self.PORT}")
        
        t.sleep(1)
        
        if self.is_tracker:
            self.heartbeat_thread = th.Thread(target=self.enviar_heartbeats)
            self.heartbeat_thread.start()


    def registrar_no_sistema(self):
        try:
            ns = Pyro5.api.locate_ns()
            if self.is_tracker:
                nome_tracker = f"Tracker_Epoca_{self.epoca}"
                ns.register(nome_tracker, self.uri)
                self._log(f"Tracker {self.peer_id} eleito para época {self.epoca}")
                for file in os.listdir(self.FILES_DIR):
                    self.registrar_arquivo(self.uri, file)
                t.sleep(2)
                ns.remove(f"Peer_{self.peer_id}")
                self.peers_conhecidos = [
                    uri for nome, uri in ns.list().items()
                    if nome.startswith("Peer_")
                ]
                self.total_peers = len(self.peers_conhecidos)
            else:
                ns.register(f"Peer_{self.peer_id}", self.uri)
                self.atualizar_tracker()
                
        except Exception as e:
            self._log(f"Erro no registro: {e}")


    @Pyro5.api.expose
    def get_peer_id(self):
        return self.peer_id


    @Pyro5.api.expose
    def ping(self):
        return True
    
    
    def verificar_tracker_periodicamente(self):
        while True:
            t.sleep(self.VERIFICACAO_TRACKER)
            if not self.is_tracker:
                self._log("Verificação periódica do tracker...")
                self.atualizar_tracker()


    def atualizar_tracker(self):
        try:
            ns = Pyro5.api.locate_ns()
            todos_registros = ns.list()
            
            trackers_validos = {}
            for nome, uri in todos_registros.items():
                if nome.startswith("Tracker_Epoca_"):
                    try:
                        with Pyro5.api.Proxy(uri) as tracker:
                            tracker._pyroTimeout = 3
                            tracker.ping()
                            epoca = int(nome.split("_")[-1])
                            trackers_validos[epoca] = uri
                    except:
                        ns.remove(nome)
                        self._log(f"Removido tracker inativo: {nome}")

            if trackers_validos:
                maior_epoca = max(trackers_validos.keys())
                self.tracker_uri = trackers_validos[maior_epoca]
                self.epoca = maior_epoca
                self._log(f"Conectado ao tracker da época {self.epoca}")
                self.registrar_arquivos_no_tracker()
            else:
                self._log("Nenhum tracker válido encontrado. Iniciando eleição...")
                self.iniciar_eleicao()
                
        except Exception as e:
            self._log(f"Erro ao atualizar tracker: {e}")
            self.iniciar_eleicao()        


    def registrar_arquivos_no_tracker(self):
        try: 
            if not self.tracker_uri:
                self.atualizar_tracker()
            with Pyro5.api.Proxy(self.tracker_uri) as tracker:
                if self.epoca_registro_arquivos < self.epoca:
                    files = os.listdir(self.FILES_DIR)
                    for file in files:
                        tracker.registrar_arquivo(self.uri, file)
                    self._log(f"Registrou {len(files)} arquivos no tracker (época {self.epoca})")
                    self.epoca_registro_arquivos = self.epoca
        except Exception as e:
            self._log(f"Falha no registro de arquivos: {e}")


    @Pyro5.api.expose
    def registrar_arquivo(self, peer_uri, filename):
        if self.is_tracker:
            self.index_arquivos[filename].add(peer_uri)
            self._log(f"Registrou {filename} de {peer_uri}")
            

    def enviar_heartbeats(self):
        while self.is_tracker:
            try:
                ns = Pyro5.api.locate_ns()
                todos_registros = ns.list()
                peers = {nome: uri for nome, uri in todos_registros.items() if nome.startswith("Peer_")}
                
                self._log(f"Enviando heartbeat para {len(peers)} peers")
                for nome_peer in peers:
                    try:
                        peer_uri = ns.lookup(nome_peer)
                        with Pyro5.api.Proxy(peer_uri) as peer:
                            peer._pyroTimeout = 3
                            peer.receber_heartbeat()
                    except Pyro5.errors.CommunicationError as e:
                        self._log(f"Falha no heartbeat para {nome_peer}: {e}")
                        ns.remove(nome_peer)
                    except Pyro5.errors.NamingError as e:
                        self._log(f"Peer {nome_peer} não encontrado: {e}")
                t.sleep(self.HEARTBEAT_INTERVALO)
            except Exception as e:
                self._log(f"Erro crítico no tracker: {e}")
                break


    @Pyro5.api.expose
    def receber_heartbeat(self):
        if self.election_timeout and not self.is_tracker:
            self._log("Heartbeat recebido")
            self.election_timeout.cancel()
            self.iniciar_detector_falhas()


    def iniciar_detector_falhas(self):
        if self.election_timeout:
            self.election_timeout.cancel()
        
        self._log(f"Próxima verificação em {self.ELEICAO_TIMEOUT:.1f} segundos")
        self.election_timeout = th.Timer(self.ELEICAO_TIMEOUT, self.iniciar_eleicao)
        self.election_timeout.start()


    def iniciar_eleicao(self):
        tracker_inativo = True
        if self.tracker_uri:
            try:
                with Pyro5.api.Proxy(self.tracker_uri) as tracker:
                    tracker._pyroTimeout = 3
                    tracker.ping()
                    tracker_inativo = False
            except Pyro5.errors.CommunicationError:
                self._log("Tracker não respondeu ao ping")
            
        if not tracker_inativo:
            self._log("Tracker ativo. Cancelando eleição.")
            self.iniciar_detector_falhas()
            return
        if self.peer_votou:
            self._log(f"Já votou na época {self.epoca + 1}")
            return
        
        self._log(f"Iniciando eleição para época {self.epoca + 1}")
        self.eleicao_em_curso = True
        self.votos_recebidos = set()
        
        try:
            ns = Pyro5.api.locate_ns()
            todos_registros = ns.list()
            self.peers_conhecidos = [
                uri for nome, uri in todos_registros.items() 
                if nome.startswith("Peer_") and nome != f"Peer_{self.peer_id}"
            ]
        except Exception as e:
            self._log(f"Erro ao buscar peers: {e}")
            self.eleicao_em_curso = False
            return

        for peer_uri in self.peers_conhecidos:
            try:
                with Pyro5.api.Proxy(peer_uri) as peer:
                    peer._pyroTimeout = 3
                    if peer.votar(self.peer_id, self.epoca + 1):
                        self.votos_recebidos.add(peer.get_peer_id())
            except Exception as e:
                self._log(f"Erro ao contactar {peer_uri}: {e}")
        
        total_necessario = (len(self.peers_conhecidos) // 2) + 1
        if len(self.votos_recebidos) >= total_necessario:
            self._log(f"Eleição concluída: {len(self.votos_recebidos)}/{total_necessario} votos")
            self.tornar_se_tracker()
        else:
            self._log(f"Eleição fracassada: {len(self.votos_recebidos)}/{total_necessario} votos")
        
        self.eleicao_em_curso = False


    @Pyro5.api.expose
    def votar(self, candidato_id, nova_epoca):
        if not self.peer_votou:
            self.peer_votou = True
            self._log(f"Votou em {candidato_id} (época {self.epoca + 1})")
            return True
        #self._log(f"Recusou voto para época {self.epoca + 1}")
        return False


    def tornar_se_tracker(self):
        self.is_tracker = True
        self.epoca += 1
        self.registrar_no_sistema()
        
        if not hasattr(self, 'heartbeat_thread') or not self.heartbeat_thread.is_alive():
            self.heartbeat_thread = th.Thread(target=self.enviar_heartbeats)
            self.heartbeat_thread.start()
        
        self._log(f"Eleito como novo tracker (época {self.epoca})")
        self.notificar_novo_tracker()


    def notificar_novo_tracker(self):
        for peer_uri in self.peers_conhecidos:
            try:
                with Pyro5.api.Proxy(peer_uri) as peer:
                    peer._pyroTimeout = 3
                    peer.atualizar_epoca(self.epoca, self.uri)
            except Exception as e:
                self._log(f"Erro ao notificar {peer_uri}: {e}")


    @Pyro5.api.expose
    def atualizar_epoca(self, nova_epoca, novo_tracker_uri):
        if nova_epoca > self.epoca:
            self.epoca = nova_epoca
            self.peer_votou = False
            self.tracker_uri = novo_tracker_uri
            self._log(f"Atualizou para época {self.epoca}")
            self.registrar_arquivos_no_tracker()
            self.iniciar_detector_falhas()


    @Pyro5.api.expose
    def baixar_arquivo(self, filename):
        path = os.path.join(self.FILES_DIR, filename)
        if os.path.exists(path):
            with open(path, "rb") as f:
                return f.read() 
        return None


    def comandos_usuario(self):
        print("\nComandos disponíveis:")
        print("buscar <arquivo> - Buscar e baixar arquivo")
        print("add <arquivo>    - Criar novo arquivo local")
        print("matar            - Encerrar este peer")
        
        while True:
            cmd = input(">> ").strip().split()
            if not cmd:
                continue
                
            if cmd[0] == "buscar" and len(cmd) == 2:
                self.baixar_arquivo(cmd[1])
            elif cmd[0] == "add" and len(cmd) == 2:
                path = os.path.join(self.FILES_DIR, cmd[1])
                with open(path, "w") as f:
                    f.write(f"Novo arquivo {cmd[1]} do peer {self.peer_id}\n")
                print(f"Arquivo {cmd[1]} criado!")
            elif cmd[0] == "matar":
                print("Encerrando peer...")
                if self.is_tracker:
                    try:
                        ns = Pyro5.api.locate_ns()
                        ns.remove(f"Tracker_Epoca_{self.epoca}")
                    except Exception as e:
                        pass
                self.daemon.shutdown()
                return
            else:
                print("Comando inválido!")


if __name__ == "__main__":
    parser = arg.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--tracker", action="store_true")
    args = parser.parse_args()
    
    peer = Peer(args.id, args.port, args.dir, args.tracker)
    peer.comandos_usuario()