function abrirModal(modal) {
  modal.style.display = 'block';
}

function fecharModal(modal) {
  modal.style.display = 'none';
}

function configurarModal(modal, closeBtn) {
  closeBtn.addEventListener('click', () => fecharModal(modal));
  window.addEventListener('click', (event) => {
    if (event.target === modal) fecharModal(modal);
  });
}

function enviarRequisicao(url, senha, mensagemProcessando, mensagemSucesso, mensagemErro) {
  const respostaEl = document.getElementById('resposta');
  const botoes = document.querySelectorAll('button');

  // Desativa todos os botões temporariamente
  botoes.forEach(btn => btn.disabled = true);

  respostaEl.innerHTML = mensagemProcessando;

  fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ senha: senha })
  })
    .then(response => {
      return response.text().then(text => {
        console.log("Resposta bruta:", text);
        try {
          return JSON.parse(text);
        } catch (e) {
          throw new Error("Não foi possível fazer parse do JSON: " + e + "\nResposta recebida:\n" + text);
        }
      });
    })
    .then(data => {
      if (data.erro) {
        respostaEl.innerHTML = "Erro: " + data.erro + criarBotaoTentarNovamente(url, senha, mensagemProcessando, mensagemSucesso, mensagemErro);
      } else {
        respostaEl.innerHTML = mensagemSucesso;
      }
    })
    .catch(error => {
      respostaEl.innerHTML = mensagemErro + criarBotaoTentarNovamente(url, senha, mensagemProcessando, mensagemSucesso, mensagemErro);
      console.error("Erro:", error);
    })
    .finally(() => {
      // Reativa os botões depois da requisição
      botoes.forEach(btn => btn.disabled = false);
    });
}

function criarBotaoTentarNovamente(url, senha, mensagemProcessando, mensagemSucesso, mensagemErro) {
  return `
    <br><button onclick="enviarRequisicao('${url}', '${senha}', '${mensagemProcessando}', '${mensagemSucesso}', '${mensagemErro}')">
      Tentar novamente
    </button>
  `;
}

// Geração de dados
const gerarBtn = document.getElementById('gerar-dados-btn');
const modalGerar = document.getElementById('senha-modal-gerar');
const closeGerar = document.querySelector('.close-gerar');
const confirmarGerarBtn = document.getElementById('confirmar-gerar');

gerarBtn.addEventListener('click', () => abrirModal(modalGerar));
configurarModal(modalGerar, closeGerar);

confirmarGerarBtn.addEventListener('click', () => {
  const senha = document.getElementById('senha-input-gerar').value;
  fecharModal(modalGerar);
  enviarRequisicao('/inserir', senha, "Gerando dados...", "Dados gerados com sucesso!", "Erro ao gerar dados!");
});

// Esvaziar dados
const esvaziarBtn = document.getElementById('esvaziar-btn');
const modal = document.getElementById('senha-modal');
const closeModal = document.querySelector('.close');
const confirmarBtn = document.getElementById('confirmar-esvaziar');

esvaziarBtn.addEventListener('click', () => abrirModal(modal));
configurarModal(modal, closeModal);

confirmarBtn.addEventListener('click', () => {
  const senha = document.getElementById('senha-input').value;
  fecharModal(modal);
  enviarRequisicao('/esvaziar', senha, "Apagando dados...", "Dados apagados com sucesso!", "Erro ao apagar dados!");
});
