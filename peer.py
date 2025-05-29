from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import Pyro5.api
import collections as c
import threading as th
import argparse as arg
import base64 as b64
import random as r
import socket as s
import time as t
import os


class Peer:
    def __init__(self, peer_id, port, files_dir, is_tracker=False):
        self.peer_id = peer_id
        self.PORT = port
        self.FILES_DIR = files_dir
        self.ENDERECO_HOST = ('localhost', 9999)
        
        self.is_tracker = is_tracker
        self.tracker_uri = None
        self.epoca = 0
        self.peers_conhecidos = []
        self.index_arquivos = c.defaultdict(list)
        
        self.election_timeout = None
        self.HEARTBEAT_INTERVALO = r.uniform(2.0, 3.0)
        self.ELEICAO_TIMEOUT = r.uniform(8.0, 10.0)
        self.VERIFICACAO_TRACKER = 10.0
        self.votos_recebidos = set()
        self.peer_votou = False
        self.eleicao_em_curso = False
        self.epoca_registro_arquivos = -1

        #monitoramento de arquivos
        self.observer = Observer()
        self.event_handler = self.FileChangeHandler(self)
        self.observer.schedule(self.event_handler, self.FILES_DIR, recursive=False)
        self.observer.start()


    def __del__(self):
        print("Encerrando peer...")
        if self.is_tracker:
            try:
                ns = Pyro5.api.locate_ns()
                ns.remove(f"Tracker_Epoca_{self.epoca}")
            except Exception as e:
                pass
        self.daemon.shutdown()
        self.observer.stop()
    
    
    class FileChangeHandler(FileSystemEventHandler):
        def __init__(self, peer):
            super().__init__()
            self.peer = peer

        def on_created(self, event):
            if not event.is_directory:
                arquivo = os.path.basename(event.src_path)
                self.peer.registrar_arquivo_local(arquivo)

        def on_deleted(self, event):
            if not event.is_directory:
                arquivo = os.path.basename(event.src_path)
                self.peer.remover_arquivo_local(arquivo)


    @Pyro5.api.expose
    def get_peer_id(self):
        return self.peer_id


    @Pyro5.api.expose
    def ping(self):
        return True
    
    
    @Pyro5.api.expose
    def registrar_arquivo(self, peer_uri, arquivo):
        if self.is_tracker:
            self.index_arquivos[arquivo].append(peer_uri)
            self._log(f"Registrou {arquivo} de {peer_uri}")
    
    
    @Pyro5.api.expose
    def remover_arquivo(self, peer_uri, arquivo):
        if self.is_tracker:
            if arquivo in self.index_arquivos:
                if peer_uri in self.index_arquivos[arquivo]:
                    self.index_arquivos[arquivo].remove(peer_uri)
                    self._log(f"Removeu {arquivo} de {peer_uri}")
                    if not self.index_arquivos[arquivo]:
                        del self.index_arquivos[arquivo]
        return True


    @Pyro5.api.expose
    def receber_heartbeat(self, epoca_tracker, tracker_uri):
        if not self.is_tracker:
            if epoca_tracker > self.epoca:
                self._log(f"Novo heartbeat da época {epoca_tracker} (atual: {self.epoca})")
                self.epoca = epoca_tracker
                self.tracker_uri = tracker_uri
                self.peer_votou = False
                self.registrar_arquivos_no_tracker()
                
            if self.election_timeout:
                self.election_timeout.cancel()
                self.iniciar_detector_falhas()
    
    
    @Pyro5.api.expose
    def votar(self, candidato_id):
        if not self.peer_votou:
            self.peer_votou = True
            self._log(f"Votou em {candidato_id} (época {self.epoca + 1})")
            return True
        #self._log(f"Recusou voto para época {self.epoca + 1}")
        return False
    
    
    @Pyro5.api.expose
    def atualizar_epoca(self, nova_epoca, novo_tracker_uri):
        if nova_epoca > self.epoca:
            self.epoca = nova_epoca
            self.tracker_uri = novo_tracker_uri
            self.peer_votou = False  
            self._log(f"Atualizou para época {self.epoca}")
            self.registrar_arquivos_no_tracker()
            self.iniciar_detector_falhas()  

    @Pyro5.api.expose
    def consultar_arquivo(self, nome_arquivo):
        return self.index_arquivos[nome_arquivo] 


    @Pyro5.api.expose
    def baixar_arquivo(self, arquivo):
        try:
            path = os.path.join(self.FILES_DIR, arquivo)
            self._log(f"Baixando arquivo {arquivo} de {self.peer_id} do caminho {path} - {os.path.exists(path)}")
            if os.path.exists(path):
                with open(path, "rb") as arq:
                    data = arq.read()
                    return data
            else:
                self._log(f"Arquivo {arquivo} não encontrado no peer {self.peer_id}")               
                return None
        except Exception as e:
            self._log(f"Erro ao ler arquivo {arquivo}: {e}")
            return None
    
    
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


    def registrar_arquivo_local(self, arquivo):
        try:
            if self.tracker_uri:
                with Pyro5.api.Proxy(self.tracker_uri) as tracker:
                    tracker.registrar_arquivo(self.uri, arquivo)
                    self._log(f"Arquivo adicionado: {arquivo} (registrado no tracker)")
        except Exception as e:
            self._log(f"Falha ao registrar {arquivo}: {e}")

    
    def remover_arquivo_local(self, arquivo):
        try:
            if self.tracker_uri:
                with Pyro5.api.Proxy(self.tracker_uri) as tracker:
                    tracker.remover_arquivo(self.uri, arquivo)
                    self._log(f"Arquivo removido: {arquivo} (excluído do tracker)")
            return True
        except Exception as e:
            self._log(f"Falha ao remover {arquivo}: {e}")


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
                            peer.receber_heartbeat(self.epoca, self.uri) 
                    except Pyro5.errors.CommunicationError as e:
                        self._log(f"Falha no heartbeat para {nome_peer}: {e}")
                        ns.remove(nome_peer)
                    except Pyro5.errors.NamingError as e:
                        self._log(f"Peer {nome_peer} não encontrado: {e}")
                t.sleep(self.HEARTBEAT_INTERVALO)
            except Exception as e:
                self._log(f"Erro crítico no tracker: {e}")
                break


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
                    if peer.votar(self.peer_id):
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


    def tornar_se_tracker(self):
        self.is_tracker = True
        self.epoca += 1
        self.registrar_no_sistema()
        
        if not hasattr(self, 'heartbeat_thread') or not self.heartbeat_thread.is_alive():
            self.heartbeat_thread = th.Thread(target=self.enviar_heartbeats)
            self.heartbeat_thread.start()
        
        self._log(f"Eleito como novo tracker (época {self.epoca})")
    

    def _log(self, message):
        try:
            with s.socket(s.AF_INET, s.SOCK_DGRAM) as sock:
                sock.sendto(f"[Peer {self.peer_id}] {message}".encode(), self.ENDERECO_HOST)
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
    

    def buscar_e_baixar_arquivo(self, arquivo):
        try:
            if not self.tracker_uri:
                self._log("Tracker não disponível para busca")
                return False
                
            with Pyro5.api.Proxy(self.tracker_uri) as tracker:
                tracker._pyroTimeout = 5
                peers = tracker.consultar_arquivo(arquivo)
                self._log(f"O arquivo {arquivo} está disponível em {len(peers)} peers")
                if not peers:
                    self._log(f"Arquivo {arquivo} não encontrado no tracker")
                    return False
                for peer_uri in peers:
                    try:
                        with Pyro5.api.Proxy(peer_uri) as peer:
                            self._log(f"Tentando baixar {arquivo} de {peer_uri}")
                            peer._pyroTimeout = 5
                            if peer.get_peer_id() == self.peer_id:
                                self._log(f"Não pode baixar de si mesmo: {peer_uri}")
                                continue
                            
                            dados_b64 = peer.baixar_arquivo(arquivo)
                            dados = b64.b64decode(dados_b64['data']) if dados_b64 else None 
                            self._log(f"{isinstance(dados, bytes)} - {dados}")
                            if dados and isinstance(dados, bytes):
                                path = os.path.join(self.FILES_DIR, arquivo)
                                with open(path, "wb") as f:
                                    f.write(dados)
                                self._log(f"Arquivo {arquivo} baixado de {peer_uri}")
                                self.registrar_arquivos_no_tracker()
                                return True
                    except Exception as e:
                        self._log(f"Falha com peer {peer_uri}: {str(e)}")
                        
                self._log(f"Todos os peers falharam para {arquivo}")
                return False
                
        except Exception as e:
            self._log(f"Erro crítico na busca: {str(e)}")
            return False


    def comandos_usuario(self):
        print("\nComandos disponíveis:")
        print("buscar <arquivo> - Buscar e baixar arquivo")
        print("add <arquivo>    - Criar novo arquivo local")
        print("mostrar          - Mostrar arquivos disponíveis")
        print("matar            - Encerrar este peer")
        
        while True:
            cmd = input(">> ").strip().split()
            if not cmd:
                continue
                
            if cmd[0] == "buscar" and len(cmd) == 2:
                self.buscar_e_baixar_arquivo(cmd[1])
            elif cmd[0] == "add" and len(cmd) == 2:
                path = os.path.join(self.FILES_DIR, cmd[1])
                with open(path, "w") as f:
                    f.write(f"Novo arquivo {cmd[1]} do peer {self.peer_id}\n")
                print(f"Arquivo {cmd[1]} criado!")
            elif cmd[0] == "mostrar" and len(cmd) == 1:
                print(self.index_arquivos)
            elif cmd[0] == "matar":
                self.__del__()
                break
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
    peer.criar_arquivos()
    peer.iniciar_servico()
    peer.registrar_no_sistema()
    
    verificador_tracker_thread = th.Thread(target=peer.verificar_tracker_periodicamente, daemon=True)
    verificador_tracker_thread.start()
    
    peer.comandos_usuario()