def zerar_tabelas():
    from db_utils import db 
    # Tabelas
    tabelas = ['hu_pedidos', 'hus', 'pedidos']
    try:
        db.ping(reconnect=True)
        cursor = db.cursor()
        for tabela in tabelas:
            cursor.execute(f"DELETE FROM {tabela}")
            cursor.execute(f"ALTER TABLE {tabela} AUTO_INCREMENT = 1")
            print(f"Tabela '{tabela}' limpa e AUTO_INCREMENT resetado.")

        db.commit()
        print("Todas as tabelas foram limpas com os IDs reiniciados.")

    except Exception as e:
        print(f"Erro ao limpar as tabelas: {e}")
        db.rollback()

    finally:
        cursor.close()
        db.close()