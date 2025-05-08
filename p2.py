import Pyro5.api as p
import threading as th
import random as r
import os

class P2P_2:
    def __init__(self):
        self.fortune_file = r.sample(os.listdir("./arquivos"), 2)
        self.lider = False
        self.tempo = 0
        
    @p.expose
    def get_texto(self, name):
        return f"Hello, {name}. Here is your fortune message: Knowledge is the beginning of practice."

daemon = p.Daemon()
objeto = P2P_2()
own_uri = daemon.register(objeto)

ns = p.locate_ns()
ns.register("p2", own_uri)
uri = ns.lookup("p1")

name = input("What is your name?\n")

greeting_maker = p.Proxy(uri)
print(greeting_maker.get_fortune(name))