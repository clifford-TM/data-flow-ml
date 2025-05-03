import random
import datetime
import string
from canalizacoes import canalizacoes


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