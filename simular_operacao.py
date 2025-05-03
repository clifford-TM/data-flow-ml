import random
import mysql.connector
from datetime import timedelta
from canalizacoes import canalizacoes
from geradores import gerar_hu, gerar_pedido, escolher_canalizacao, escolher_etd 
from db import db


# Cursos para executar comandos sql
cursor = db.cursor()

# Dicionários para gerenciamento de HUs

hu_ativas = {} # Gerencia hus com status "Aberto"

hu_pedidos_count = {} # Gerencia a quantidade de pedidos associada a cada hu

hu_limites = {} # Gerencia o limite de pedidos que podem ser atrelados em uma hu

hu_datas = {} # Gerencia data_criacao e data_final de cada hu

hu_canalizacoes = {} # Gerencia em qual canalização a hu foi criada


# Por aqui podemos controlar a quantidade de desvios da simulação
def gerar_aging():
    chance = random.random()  # Gera número entre 0.0 e 1.0
    if chance < 0.99:
        return round(random.uniform(1.0, 4.0), 2)
    else:
        return round(random.uniform(5.0, 10.0), 2)


# Função para executar comandos SQL sem perder a conexão (necessária pelo ambiente azure)
def executar_sql(query, valores=None, fetch=False):
    try:
        # Refaz a conexão com o banco de dados
        db.ping(reconnect=True)

        # Executa o comando
        if valores:
            cursor.execute(query, valores)
        else:
            cursor.execute(query)

        # Executa a query SQL e retorna o resultado. Se `fetch` for "all", retorna todas as linhas; caso contrário, retorna a próxima linha. 
        # Se `fetch` for False ou None, não retorna nada.
        if fetch:
            return cursor.fetchall() if fetch == "all" else cursor.fetchone()
        
    # tratamento de erro
    except mysql.connector.Error as err:
        print(f"[ERRO MySQL] {err}")
        raise


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


def simular():
    try:
        hus_abertas = executar_sql("SELECT hu, canalizacao, data_criacao, data_final FROM hus WHERE status = 'Aberto'", fetch="all")
        for hu, canalizacao, data_criacao, data_final in hus_abertas:
            if data_final is None:
                data_final = data_criacao + timedelta(hours=1)

            hu_ativas[canalizacao] = hu
            hu_datas[hu] = (data_criacao, data_final)
            count = executar_sql("SELECT COUNT(*) FROM hu_pedidos WHERE hu = %s", (hu,), fetch=True)[0]
            hu_pedidos_count[hu] = count
            hu_limites[hu] = random.randint(80, 130)

        pedidos_gerados = 0
        total_pedidos = 500

        while pedidos_gerados < total_pedidos:
            pedido = gerar_pedido()
            aging = gerar_aging()
            canalizacao = escolher_canalizacao()
            etd, hora = escolher_etd(canalizacao)
            rampa = random.choice(canalizacoes[canalizacao]["rampas"])
            hu = hu_ativas.get(canalizacao)


            # Se não houver cointainer ou ele estiver cheio
            if not hu or hu_pedidos_count[hu] >= hu_limites[hu]:
                hu = criar_nova_hu(canalizacao, etd)
                data_criacao, data_final = executar_sql(
                    "SELECT data_criacao, data_final FROM hus WHERE hu = %s", (hu,), fetch=True
                )
                hu_datas[hu] = (data_criacao, data_final)
                hu_ativas[canalizacao] = hu
                hu_pedidos_count[hu] = 0

            data_criacao, data_final = hu_datas[hu]

            delta = int((data_final - data_criacao).total_seconds())
            segundos_random = random.randint(0, max(10, delta - 100))

            if aging >= 5:
                data_desvio = data_criacao + timedelta(seconds=segundos_random)
                desviar_pedido(pedido, etd, canalizacao, aging, data_desvio, rampa, hora)


            # Caso haja uma HU que possa receber pedidos
            else:
                data_atrelamento = data_criacao + timedelta(seconds=segundos_random)
                atrelar_pedido(pedido, hu, etd, canalizacao, aging, data_atrelamento, rampa, hora)

                # Atualizar a contagem de pacotes
                atualizar_pacotes(hu)
                hu_pedidos_count[hu] += 1

            status = "atrelado" if aging < 5 else "desviado"
            print(f"Pedido: {pedido}, Canalização: {canalizacao}, HU: {hu}, Status: {status}")
            pedidos_gerados += 1

        db.commit()
        print(f"{pedidos_gerados} pedidos processados.")

    finally:
        cursor.close()
        db.close()