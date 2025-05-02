import os
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from simular_operacao import simular
from zerar_tabelas import zerar_tabelas
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)


# senha da aplicação
SENHA = os.getenv('SENHA')

# rota para renderizar admin.html
@app.route('/')
def index():
    return render_template('admin.html')

# Post para inserir dados no banco de dados
@app.route('/inserir', methods=['POST'])
def inserir():
    senha = request.json.get('senha')

    if senha != SENHA:
        return jsonify({'erro': 'Senha incorreta'}), 403

    try:
        simular()
        return jsonify({'mensagem': 'Dados gerados com sucesso!'}), 200
    except Exception as e:
        print('Erro no script:', e)
        return jsonify({'erro': f'Erro ao executar script: {str(e)}'}), 500



# Post para esvaziar o banco de dados (requer senha)
@app.route('/esvaziar', methods=['POST'])
def esvaziar():
    try:
        # Verifica a senha enviada na requisição
        senha = request.json.get('senha')
        if senha != SENHA:
            return jsonify({'erro': 'Senha incorreta!'}), 403  # HTTP Forbidden
        
        # Zera o banco de dados
        zerar_tabelas()  
        return jsonify({'mensagem': 'Dados apagados com sucesso!'}), 200
    
    # tratamento de erro
    except Exception as e:
        print('Erro no script:', e)
        return jsonify({'erro': f'Erro ao executar script: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)
