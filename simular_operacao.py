import random
import time
from datetime import timedelta
from geradores import gerar_pedido, gerar_aging, escolher_canalizacao, escolher_etd, escolher_rampa 
from db_utils import db, executar_sql
from gerenciador_pacotes import hus, obter_hu, criar_nova_hu, atrelar_pedido, desviar_pedido, atualizar_pacotes

cursor = db.cursor()

def gerenciar_hus():
    hus_abertas = executar_sql("SELECT hu, canalizacao, data_criacao, data_final FROM hus WHERE status = 'Aberto'", fetch="all")
    for hu, canalizacao, data_criacao, data_final in hus_abertas:
        if data_final is None:
            data_final = data_criacao + timedelta(hours=1)

        count = executar_sql("SELECT COUNT(*) FROM hu_pedidos WHERE hu = %s", (hu,), fetch=True)[0]
        limite = random.randint(80, 130)

        hus[hu] = {
            "canalizacao": canalizacao,
            "status": "Aberto",
            "pedidos_count": count,
            "limite": limite,
            "data_criacao": data_criacao,
            "data_final": data_final
        }

def gerar_simulacao():
    pedido = gerar_pedido()
    aging = gerar_aging()
    canalizacao = escolher_canalizacao()
    etd, hora = escolher_etd(canalizacao)
    rampa = escolher_rampa(canalizacao)
    hu = obter_hu(canalizacao)

    return pedido, aging, canalizacao, etd, hora, rampa, hu

def processar_dados(pedido, aging, canalizacao, etd, hora, rampa, hu):
    # Se não houver cointainer ou ele estiver cheio
    if not hu or hus[hu]["pedidos_count"] >= hus[hu]["limite"]:
        hu = criar_nova_hu(canalizacao, etd)
    
    data_criacao = hus[hu]["data_criacao"]
    data_final =  hus[hu]["data_final"] 
    delta = int((data_final - data_criacao).total_seconds())
    segundos_random = random.randint(0, max(10, delta - 100))

    if aging >= 5:
        data_desvio = data_criacao + timedelta(seconds=segundos_random)
        desviar_pedido(pedido, etd, canalizacao, aging, data_desvio, rampa, hora)
        status = "desviado"
    else:
        data_atrelamento = data_criacao + timedelta(seconds=segundos_random)
        atrelar_pedido(pedido, hu, etd, canalizacao, aging, data_atrelamento, rampa, hora)
        atualizar_pacotes(hu)
        hus[hu]["pedidos_count"] += 1

    return hu

# Função para refatorar e não esgotar os recursos
def respiro_blocos():
    time.sleep(random.uniform(0.3, 1.2))  # de 300ms a 1.2s

def simular():
    try:
        gerenciar_hus()

        pedidos_gerados = 0
        total_pedidos = 700
        intervalo_commit = 100 # commit para o db a cada 100 pedidos

        while pedidos_gerados < total_pedidos:
            pedidos_info = gerar_simulacao()
            processar_dados(*pedidos_info)
            #respiro_blocos()

            pedidos_gerados += 1

            # Para evitar o erro de timeout fazemos commits divididos usando a lógica de módulo %
            # Esse cálculo verifica quando o mútiplo de intervalo foi atingido
            if pedidos_gerados % intervalo_commit == 0:
                db.commit()
                print(f"Commit intermediário {pedidos_gerados} gerados.")

        db.commit()
        print(f"{pedidos_gerados} pedidos processados.")

    finally:
        cursor.close()
        db.close()