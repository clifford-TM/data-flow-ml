import os
import random
import string
import mysql.connector
from datetime import datetime, timedelta
from canalizacoes import canalizacoes
from dotenv import load_dotenv


load_dotenv()


# Definindo a conexão cocm o banco de dados
conexao = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),      
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=int(os.getenv("DB_PORT"))    
)

print("Tentando conectar com os seguintes dados:")
print("HOST:", os.getenv("DB_HOST"))
print("USER:", os.getenv("DB_USER"))
print("PASSWORD:", os.getenv("DB_PASSWORD"))
print("DB:", os.getenv("DB_NAME"))
print("PORT:", os.getenv("DB_PORT"))


# Cursos para executar comandos sql
cursor = conexao.cursor()

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


# Gerenciando HUs
def criar_nova_hu(canalizacao, etd):
    hu_antiga = hu_ativas.get(canalizacao)

    # A hu deve começar entre 1h30 e 5h antes do ETD
    horas = random.randint(1, 4)
    minutos = random.choice([10, 15, 30])
    data_criacao = etd - timedelta(hours=horas, minutes=minutos)

    # Calculando data_final
    tempo_estimado = (etd - data_criacao).total_seconds()
    duracao = random.randint(20 * 60, int(tempo_estimado))
    data_final = data_criacao + timedelta(seconds=duracao)

   

    if hu_antiga and hu_antiga in hu_datas:
        data_criacao_antiga = hu_datas[hu_antiga][0]
        data_final_antiga = hu_datas[hu_antiga][1]

        print(f"HU: {hu_antiga}, data_criacao: {data_criacao_antiga}, data_final: {data_final_antiga}")

        cursor.execute(
            "UPDATE hus SET status = %s, data_final = %s WHERE hu = %s",
            ("Finalizado", data_final_antiga, hu_antiga)
        )
        conexao.commit()

    nova_hu = gerar_hu()
    posicao = random.choice(canalizacoes[canalizacao]["rampas"])
    limite_pedidos = random.randint(80, 130)


    

    cursor.execute(
        "INSERT INTO hus (hu, status, etd, pacotes, canalizacao, posicao, data_criacao, data_final) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (nova_hu, "Aberto", etd, 0, canalizacao, posicao, data_criacao, data_final)
    )
    conexao.commit()

    hu_ativas[canalizacao] = nova_hu
    hu_canalizacoes[nova_hu] = canalizacao
    hu_pedidos_count[nova_hu] = 0
    hu_limites[nova_hu] = limite_pedidos
    hu_datas[nova_hu] = (data_criacao, data_final)


    return nova_hu


def simular():
    try:
        # Carregando HUs abertas no banco de dados
        cursor.execute("SELECT hu, canalizacao, data_criacao, data_final FROM hus WHERE status = 'Aberto'")
        hus_abertas = cursor.fetchall()

        for hu, canalizacao, data_criacao, data_final in hus_abertas:
            if data_final is None:
                data_final = data_criacao + timedelta(hours=1)
            
            hu_ativas[canalizacao] = hu
            hu_datas[hu] = (data_criacao, data_final)

            # Carrega a contagem atual de pedidos da HU
            cursor.execute("SELECT COUNT(*) FROM hu_pedidos WHERE hu = %s", (hu,))
            count = cursor.fetchone()[0]
            hu_pedidos_count[hu] = count

            # Define um novo limite aleatório
            hu_limites[hu] = random.randint(80, 130)

        pedidos_gerados = 0
        total_pedidos = 1000
        pedidos_processados = []

        while pedidos_gerados < total_pedidos:
            pedido = gerar_pedido()
            aging = gerar_aging()
            canalizacao = escolher_canalizacao()
            etd, hora = escolher_etd(canalizacao)
            desvio = None
            rampa = random.choice(canalizacoes[canalizacao]["rampas"])

            hu = hu_ativas.get(canalizacao)

            # Se não houver HU ativa ou ela estiver cheia, criamos uma nova HU
            if not hu or hu_pedidos_count[hu] >= hu_limites[hu]:
                hu = criar_nova_hu(canalizacao, etd)

                # Recarrega datas da nova HU
                cursor.execute("SELECT data_criacao, data_final FROM hus WHERE hu = %s", (hu,))
                data_criacao, data_final = cursor.fetchone()
                hu_datas[hu] = (data_criacao, data_final)
                hu_ativas[canalizacao] = hu
                hu_pedidos_count[hu] = 0

            data_criacao, data_final = hu_datas[hu]

            if aging >= 5:
                # DESVIO dentro da HU
                delta = int((data_final - data_criacao).total_seconds())
                segundos_random = random.randint(0, max(10, delta - 100))
                desvio = data_criacao + timedelta(seconds=segundos_random)

                cursor.execute(
                    "INSERT INTO pedidos (pedido, etd, canalizacao, aging, atrelamento, desvio, hr, rampa, hora) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (pedido, etd, canalizacao, str(aging), None, desvio, hora, rampa, hora.hour)
                )

                pedidos_processados.append(f"{pedido} (desviado, aging {aging})")
            else:
                # ATRELAMENTO dentro da HU
                delta = int((data_final - data_criacao).total_seconds())
                segundos_random = random.randint(0, max(10, delta - 100))
                data_atrelamento = data_criacao + timedelta(seconds=segundos_random)

                cursor.execute(
                    "INSERT INTO pedidos (pedido, etd, canalizacao, aging, atrelamento, desvio, hr, rampa, hora) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (pedido, etd, canalizacao, str(aging), data_atrelamento, None, hora, rampa, hora.hour)
                )

                cursor.execute(
                    "INSERT INTO hu_pedidos (hu, pedido, data_atrelamento) VALUES (%s, %s, %s)",
                    (hu, pedido, data_atrelamento)
                )

                cursor.execute(
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

                hu_pedidos_count[hu] += 1
                pedidos_processados.append(f"{pedido} → HU {hu} (canalização {canalizacao})")

            status = "atrelado" if aging < 5 else "desviado"
            print(f"Pedido: {pedido}, Canalização: {canalizacao}, HU: {hu}, Status: {status}")
            pedidos_gerados += 1

        conexao.commit()
        print(f"{pedidos_gerados} pedidos processados.")

    except KeyboardInterrupt:
        print("\nSimulação encerrada manualmente.")
    finally:
        cursor.close()
        conexao.close()