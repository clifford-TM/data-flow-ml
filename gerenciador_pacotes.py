import random
from datetime import timedelta
from db_utils import db, executar_sql
from geradores import gerar_hu
from canalizacoes import canalizacoes

# Dicionário para gerenciamento de HUs 
hus = {}

# Obtem a próxima Hu dos iterável hus em que o status esteja aberto para uma canalização informada
def obter_hu(canalizacao):
    return next((hu for hu, dados in hus.items() if dados["canalizacao"] == canalizacao and dados["status"] == "Aberto"), None)

# Gerenciando HUs
def criar_nova_hu(canalizacao, etd):
    hu_antiga = obter_hu(canalizacao)
    horas = random.randint(1, 4)
    minutos = random.choice([10, 15, 30])
    data_criacao = etd - timedelta(hours=horas, minutes=minutos)
    tempo_estimado = (etd - data_criacao).total_seconds()
    duracao = random.randint(20 * 60, int(tempo_estimado))
    data_final = data_criacao + timedelta(seconds=duracao)

    if hu_antiga:
        hus[hu_antiga]["status"] = "Finalizado"
        hus[hu_antiga]["data_final"] = data_final

        executar_sql(
            "UPDATE hus SET status = %s, data_final = %s WHERE hu = %s",
            ("Finalizado", data_final, hu_antiga)
        )

    nova_hu = gerar_hu()
    posicao = random.choice(canalizacoes[canalizacao]["rampas"])
    limite_pedidos = random.randint(80, 130)

    hus[nova_hu] = {
        "canalizacao": canalizacao,
        "status": "Aberto",
        "pedidos_count": 0,
        "limite": limite_pedidos,
        "data_criacao": data_criacao,
        "data_final": data_final
    }

    executar_sql(
        "INSERT INTO hus (hu, status, etd, pacotes, canalizacao, posicao, data_criacao, data_final) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (nova_hu, "Aberto", etd, 0, canalizacao, posicao, data_criacao, data_final)
    )

    return nova_hu

# Criando pedidos, desvios e atualizando o banco
def atrelar_pedido(pedido, hu, etd, canalizacao, aging, atrelamento, rampa, hora):
    # Inserindo pedidos na tabela pedidos
    executar_sql(
        "INSERT INTO pedidos (pedido, etd, canalizacao, aging, atrelamento, desvio, hr, rampa, hora) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (pedido, etd, canalizacao, str(aging), atrelamento, None, hora, rampa, hora.hour)
    )

    # Atualizando a tabela relacional hu_pedidos
    executar_sql(
        "INSERT INTO hu_pedidos (hu, pedido, data_atrelamento) VALUES (%s, %s, %s)",
        (hu, pedido, atrelamento)
    )

def desviar_pedido(pedido, etd, canalizacao, aging, desvio, rampa, hora):
    # O desvio será contabilizado porém não atrelado
    executar_sql(
        "INSERT INTO pedidos (pedido, etd, canalizacao, aging, atrelamento, desvio, hr, rampa, hora) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (pedido, etd, canalizacao, str(aging), None, desvio, hora, rampa, hora.hour)
    )

def atualizar_pacotes(hu):
    executar_sql(
        """
        UPDATE hus
        SET pacotes = (
            SELECT COUNT(*)
            FROM hu_pedidos
            WHERE hu = %s
        )
        WHERE hu = %s
        """,
        (hu, hu)
                )
