import Pyro5.api as p
import threading as th
import random as r
import os

class P2P_1:
    def __init__(self):
        self.fortune_file = r.sample(os.listdir("./shared"), 2)
        self.lider = False
        self.tempo = 0
        
    @p.expose
    def get_fortune(self, name):
        return f"Hello, {name}. Here is your fortune message: Knowledge is the beginning of practice."

daemon = p.Daemon()
objeto = P2P_1()
own_uri = daemon.register(objeto)

# Registro
ns = p.locate_ns()
ns.register("p1", own_uri)

print(f"Ready. Object uri = {own_uri}")
daemon.requestLoop()