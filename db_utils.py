import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

db_env = "server"

if db_env == "server":
    # Banco de dados servidor (teste no servidor)
    db = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),      
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT"))    
    )
elif db_env == "local":
    # Banco de dados local (para testes de operações sql)
    db = mysql.connector.connect(
        host=os.getenv("LOCAL_HOST"),
        user=os.getenv("LOCAL_USER"),      
        password=os.getenv("LOCAL_PASSWORD"),
        database=os.getenv("LOCAL_NAME"), 
    )
    

# Cursor para executar comandos sql
cursor = db.cursor()




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
        else:
            # Se não for select fazemos um commit
            db.commit()
        
    # tratamento de erro
    except mysql.connector.Error as err:
        print(f"[ERRO MySQL] {err}")
        db.rollback()
        raise

# Definindo lotes para execução de múltiplos comandos
lote = []
batch_size = 10

def adicionar_batch(query, valores):
    global batch
    batch.append((query, valores))
    
    if len(batch) >= batch_size:
        executar_batch()

# Função para executar todos os comandos em batch
def executar_batch():
    global batch
    try:
        db.ping(reconnect=True)
        for query, valores in batch:
            if valores:
                cursor.execute(query, valores)
            else:
                cursor.execute(query)
                
        db.commit()
        batch = []
    except mysql.connector.Error as err:
        print(f"[ERRO MySQL - batch] {err}")
        db.rollback()


# Execução imediata para ações criticas
def execucao_imediata(query, valores=None, fetch=False):
    try:
        db.ping(reconnect=True)
        cursor.execute(query, valores)
        db.commit()

        if fetch:
            return cursor.fetchall() if fetch == "all" else cursor.fetchone()
    except mysql.connector.Error as err:
        print(f"[ERRO MySQL] {err}")
        raise
