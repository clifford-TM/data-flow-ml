import random
from datetime import timedelta
from geradores import gerar_pedido, gerar_aging, escolher_canalizacao, escolher_etd, escolher_rampa 
from db_utils import db, cursor, executar_sql
from gerenciador_pacotes import hus, obter_hu, criar_nova_hu, atrelar_pedido, desviar_pedido, atualizar_pacotes


def simular():
    try:
        hus_abertas = executar_sql("SELECT hu, canalizacao, data_criacao, data_final FROM hus WHERE status = 'Aberto'", fetch="all")
        for hu, canalizacao, data_criacao, data_final in hus_abertas:
            if data_final is None:
                data_final = data_criacao + timedelta(hours=1)

            count = executar_sql("SELECT COUNT(*) FROM hu_pedidos WHERE hu = %s", (hu,), fetch=True)[0]
            limite = random.randint(80, 130)

            hus[hu] = {
                "canalizacao": canalizacao,
                "status": status,
                "pedidos_count": count,
                "limite": limite,
                "data_criacao": data_criacao,
                "data_final": data_final
            }

        pedidos_gerados = 0
        total_pedidos = 500

        while pedidos_gerados < total_pedidos:
            pedido = gerar_pedido()
            aging = gerar_aging()
            canalizacao = escolher_canalizacao()
            etd, hora = escolher_etd(canalizacao)
            rampa = escolher_rampa(canalizacao)
            hu = obter_hu(canalizacao)


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


            # Caso haja uma HU que possa receber pedidos
            else:
                data_atrelamento = data_criacao + timedelta(seconds=segundos_random)
                atrelar_pedido(pedido, hu, etd, canalizacao, aging, data_atrelamento, rampa, hora)

                # Atualizar a contagem de pacotes
                atualizar_pacotes(hu)
                hus[hu]["pedidos_count"] += 1

            status = "atrelado" if aging < 5 else "desviado"
            print(f"Pedido: {pedido}, Canalização: {canalizacao}, HU: {hu}, Status: {status}")
            pedidos_gerados += 1

        db.commit()
        print(f"{pedidos_gerados} pedidos processados.")

    finally:
        cursor.close()
        db.close()