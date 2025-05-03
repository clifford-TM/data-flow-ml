import random
import datetime
import string
from canalizacoes import canalizacoes

# Por aqui podemos controlar a quantidade de desvios da simulação
def gerar_aging():
    chance = random.random()  # Gera número entre 0.0 e 1.0
    if chance < 0.99:
        return round(random.uniform(1.0, 4.0), 2)
    else:
        return round(random.uniform(5.0, 10.0), 2)
    
# Parametros de construção de pedidos e HU
def gerar_pedido():
    return ''.join(random.choices(string.digits, k=11))

def gerar_hu():
    return ''.join(random.choices(string.digits, k=20))

def escolher_canalizacao():
    return random.choice(list(canalizacoes.keys()))

def escolher_etd(canalizacao):
    horarios = canalizacoes[canalizacao]["horarios"]
    hora_escolhida = random.choice(horarios)
    agora = datetime.now()
    etd = datetime.combine(agora.date(), hora_escolhida)
    return etd, hora_escolhida

def escolher_rampa(canalizacao):
    rampa = random.choice(canalizacoes[canalizacao]["rampas"])
    return rampa
