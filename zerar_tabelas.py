def zerar_tabelas():
    import mysql.connector
    import os
    from dotenv import load_dotenv
    from pathlib import Path



    env_path = Path(__file__).resolve().parents[2] / '.env'
    load_dotenv(dotenv_path=env_path)


# Definindo a conexão cocm o banco de dados
    db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),  # Valor padrão: localhost
    user=os.getenv("DB_USER"),       # Valor padrão: davi
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=int(os.getenv("DB_PORT"))     # Convertendo para inteiro
)

    # Cursor
    cursor = db.cursor()

    # Tabelas
    tabelas = ['hu_pedidos', 'hus', 'pedidos']
    try:
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