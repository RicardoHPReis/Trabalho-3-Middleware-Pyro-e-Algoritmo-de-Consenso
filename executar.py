import subprocess as sub
import os 

# Abrir o servidor de nomes do Pyro5
sub.Popen(f'start cmd /k python -m Pyro5.nameserver', shell=True)

# Abrir os scripts em janelas de terminal separadas
pasta_sistema = os.path.dirname(os.path.abspath(__file__))
scripts = ["p1.py", "p2.py"]

for script in scripts:
    caminho_completo = os.path.join(pasta_sistema, script)
    sub.Popen(f'start cmd /k python "{caminho_completo}"', shell=True)