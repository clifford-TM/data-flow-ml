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