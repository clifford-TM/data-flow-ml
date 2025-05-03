import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# Definindo a conexão cocm o banco de dados
db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),      
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=int(os.getenv("DB_PORT"))    
)

# Cursos para executar comandos sql
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
        
    # tratamento de erro
    except mysql.connector.Error as err:
        print(f"[ERRO MySQL] {err}")
        raise