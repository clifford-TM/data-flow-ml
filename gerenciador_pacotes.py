import random
from datetime import timedelta
from db_utils import db, executar_sql
from geradores import gerar_hu
from canalizacoes import canalizacoes

# Dicionários para gerenciamento de HUs

hu_ativas = {} # Gerencia hus com status "Aberto"

hu_pedidos_count = {} # Gerencia a quantidade de pedidos associada a cada hu

hu_limites = {} # Gerencia o limite de pedidos que podem ser atrelados em uma hu

hu_datas = {} # Gerencia data_criacao e data_final de cada hu

hu_canalizacoes = {} # Gerencia em qual canalização a hu foi criada

# Gerenciando HUs
def criar_nova_hu(canalizacao, etd):
    hu_antiga = hu_ativas.get(canalizacao)
    horas = random.randint(1, 4)
    minutos = random.choice([10, 15, 30])
    data_criacao = etd - timedelta(hours=horas, minutes=minutos)
    tempo_estimado = (etd - data_criacao).total_seconds()
    duracao = random.randint(20 * 60, int(tempo_estimado))
    data_final = data_criacao + timedelta(seconds=duracao)

    if hu_antiga and hu_antiga in hu_datas:
        data_final_antiga = hu_datas[hu_antiga][1]
        executar_sql(
            "UPDATE hus SET status = %s, data_final = %s WHERE hu = %s",
            ("Finalizado", data_final_antiga, hu_antiga)
        )
        db.commit()

    nova_hu = gerar_hu()
    posicao = random.choice(canalizacoes[canalizacao]["rampas"])
    limite_pedidos = random.randint(80, 130)

    executar_sql(
        "INSERT INTO hus (hu, status, etd, pacotes, canalizacao, posicao, data_criacao, data_final) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (nova_hu, "Aberto", etd, 0, canalizacao, posicao, data_criacao, data_final)
    )
    db.commit()

    hu_ativas[canalizacao] = nova_hu
    hu_canalizacoes[nova_hu] = canalizacao
    hu_pedidos_count[nova_hu] = 0
    hu_limites[nova_hu] = limite_pedidos
    hu_datas[nova_hu] = (data_criacao, data_final)

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
