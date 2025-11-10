# 📄 **PROA Scraper & PDF Automation (Python – Jupyter Notebook)**

Automação completa para análise de processos PROA/RS, extração de PDFs, integração com Google Drive e atualização de planilhas no Google Sheets — tudo em um único **notebook monolítico**.

---

## 🚀 **Visão Geral**

Este projeto é uma solução **100% automatizada**, construída em **Python dentro de um Jupyter Notebook**, para processar documentos do **PROA/RS**, extrair dados relevantes de PDFs, consultar o status do processo via web scraping e atualizar automaticamente uma planilha no **Google Sheets** com todas as informações consolidadas.

O notebook funciona como um **pipeline ETL completo**:

* **Extract:** lê PDFs, raspa dados do PROA, extrai eventos do expediente
* **Transform:** trata textos, interpreta multas, converte datas e prazos
* **Load:** envia os dados para o Google Sheets, com hiperlinks automáticos para o PDF original

Tudo isso rodando em **um único arquivo**, simples de entender e fácil de versionar.

---

## 🧠 **O que o sistema faz atualmente**

### ✔️ 1. Processamento completo de PDFs

Usando **PyMuPDF (fitz)**, o notebook:

* lê o texto integral de cada PDF
* encontra automaticamente:

  * PROA notificatório
  * PROA mãe
  * CNPJ
  * empresa
  * número do contrato
  * tipo de penalidade
  * percentual da multa
  * CFIL/RS
  * prazo da penalidade
  * data do expediente (rodapé)
* interpreta números por extenso (um, dois, três…)
* detecta erros e PDFs mal formados

### ✔️ 2. Consulta ao status oficial no site do PROA

Com **requests** + **BeautifulSoup**, o notebook acessa o portal público do PROA e obtém o **status mais recente** do processo.

Inclui:

* leitura robusta do HTML
* tratamento de falhas
* mensagens de erro claras
* delay automático de 3s para evitar bloqueio do servidor

### ✔️ 3. Geração automática de hiperlinks no Google Sheets

O notebook:

* encontra os PDFs correspondentes no **Google Drive**
* extrai o link de compartilhamento
* transforma o valor da célula (PROA ou nome) em:

  ```
  =HYPERLINK("https://drive..."; "Nome do Processo")
  ```
* fallback automático caso o link não seja encontrado

### ✔️ 4. Escrita segura no Google Sheets

Com **gspread**, o notebook:

* sincroniza cada linha por PROA (upsert)
* limpa lixo e duplicatas
* organiza e padroniza dados
* remove validações antigas
* garante consistência e segurança

### ✔️ 5. Pipeline idempotente

Você pode rodar o notebook quantas vezes quiser:
**o resultado sempre será consistente**.

---

## 🔮 Funcionalidades Futuras (Roadmap)

### 🟦 1. **Playwright** para baixar os PDFs automaticamente

Planejamento para o próximo módulo:

* acessar automaticamente o portal
* fazer login (se necessário)
* baixar os PDFs novos
* verificar se houve atualização nos processos
* mover arquivos para pasta correta
* executar o pipeline de extração automaticamente

Isso tornará toda a automação **completamente autônoma, ponta a ponta**.

### 🟦 2. Pré-classificação inteligente de PDFs

* detectar tipo do documento
* renomear arquivos
* separar processos automaticamente

### 🟦 3. Dashboard analítico (Sheets ou Web)

* gráficos de multas
* empresas reincidentes
* % de penalidades
* métricas de tempo e volume

---

## 🏗️ Estrutura do Projeto (Monolito Jupyter)

Como é um **monólito**, tudo está organizado dentro de um único notebook:

```
📁 proa-scraper-notebook/
│
├── PROA_Automation.ipynb       # Notebook principal (pipeline completo)
│
├── pdfs/                       # PDFs a serem processados
│   ├── *.pdf
│
├── credentials/                # Credencial do Google
│   └── service_account.json
│
├── README.md
└── requirements.txt
```

Dentro do notebook, o código está organizado em células:

### 🔹 **1. Configuração e autenticação**

* imports
* setup das APIs de Google Drive e Sheets
* definição dos caminhos

### 🔹 **2. Funções utilitárias**

* normalização de texto
* regex avançado
* parsing de PDF
* conversão de datas

### 🔹 **3. Extração do Expediente**

* recorte da página correta
* extração dos dados relevantes

### 🔹 **4. Web scraping do PROA**

* requisição HTTP com fallback
* parse do HTML
* interpretação do status

### 🔹 **5. Link do Google Drive**

* busca do PDF correspondente
* criação do hiperlink

### 🔹 **6. Atualização do Google Sheets**

* upsert
* limpeza
* formatação

### 🔹 **7. Execução final**

* processamento sequencial
* logs claros
* exibição final do DataFrame

---

## 🧰 Tecnologias Utilizadas

* **Python 3.10+**
* **Jupyter Notebook**
* **PyMuPDF (fitz)** → leitura de PDFs
* **BeautifulSoup4** + **requests** → scraping do PROA
* **regex avançado**
* **google-auth / gspread / google-api-python-client**
* **pandas**
* **Playwright (planejado)**

---

## ▶️ Como Executar

### 1. Instale as dependências

```
pip install -r requirements.txt
```

### 2. Adicione as credenciais na pasta:

```
credentials/service_account.json
```

### 3. Coloque seus PDFs em:

```
pdfs/
```

### 4. Abra o notebook:

```
jupyter notebook PROA_Automation.ipynb
```

### 5. Execute as células na ordem

O notebook já contém logs claros e todos os passos explicados.

---

## 🧩 Por que este projeto existe?

Gerenciar documentos, consultar PROAs e atualizar planilhas manualmente é:

* repetitivo
* demorado
* propenso a erros

Este notebook centraliza todo o fluxo em um só lugar, tornando o processo:

* mais rápido
* mais seguro
* mais organizado
* e fácil de manter ou estender

---

## ❤️ Contribuições

Mesmo sendo um monólito pessoal, contribuições, melhorias e sugestões são bem-vindas.

