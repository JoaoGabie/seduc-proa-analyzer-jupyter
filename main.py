# ======================INICIO=DO=STATUS===========================================
# 1. Instala as bibliotecas necessárias
!pip install requests beautifulsoup4 --quiet
!pip install gspread gspread-dataframe google-auth --quiet
!pip install pymupdf --quiet

import requests
from bs4 import BeautifulSoup
import re
import os
import datetime
import pandas as pd
import fitz  # pymupdf
import tiktoken
import time
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.colab import auth
from google.auth import default
from googleapiclient.discovery import build

# ==============================================================================
# 1. AUTENTICAÇÃO E CONFIGURAÇÃO DA API
# ==============================================================================
print("🔐 Autenticando usuário...")
auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)
drive = build('drive', 'v3', credentials=creds)
print("✅ Autenticação concluída!")

# ==========================
# CONFIGURAÇÃO DE PASTAS E PLANILHAS
# ==========================
PDF_DIR = "/content/drive/MyDrive/Automação & Processos – DMOE-SEDUC/Processo Notificatório/PDF"
PLANILHA_DIR = "/content/drive/MyDrive/Automação & Processos – DMOE-SEDUC/Processo Notificatório/Processo_Notificatorio_DMOE.gsheet"
GSHEET_NAME = os.path.splitext(os.path.basename(PLANILHA_DIR))[0]
GSHEET_WORKSHEET_NAME = "TABELA"

# ID DA PASTA DO DRIVE (Aquele que funcionou para você)
FOLDER_ID_DRIVE = "1hl0liZWvMfr1GLzm9_PO9om_7fErJa_5"

# ======= MENSAGENS DE ERROS ========
ERR_MSG_EXPEIDENTE = "Sem Penalidade"
ERR_MSG_TIPO_PENALIDADE  = ""
ERR_MSG_PERCENTUAL_MULTA  = ""
ERR_MSG_IMPEDIMENTOS = ""
ERR_MSG_PENALIDADE_MESES = ""
ERR_MSG_DATA_PENALIZACAO = ""
ERR_MSG_STATUS = "ERRO: IMPOSSIVEL DE DEFINIR UM STATUS"

# ====== NOME DAS COLUNAS PADRÃO ======
COLUMNS = [
    "numero_contrato", "nome_empresa", "cnpj_empresa", "proa_notificatorio",
    "proa_mae", "status_processo", "valor_contrato_consolidado",
    "tipo_penalidade", "percentual_multa", "valor_multa", "impedimentos",
    "penalidade_meses", "data_penalizacao", "ultima_analise_feita",
    "ultima_atualizacao_processo"
]

# ==========================
# FUNÇÕES AUXILIARES DE TEXTO E REGEX
# ==========================
def extract_pdf_text(pdf_path: str) -> str:
    """Extrai TODO o texto do PDF página a página."""
    try:
        doc = fitz.open(pdf_path)
        pages_text = [page.get_text("text") for page in doc]
        doc.close()
        return "\n".join(pages_text)
    except Exception:
        return ""

def _norm_text(s: str) -> str:
    s = (s.replace("\xa0", " ").replace("\u2009", " ").replace("\u200a", " ")
           .replace("\u200b", "").replace("–", "-").replace("—", "-").replace("-", "-"))
    s = re.sub(r"[ \t]+", " ", s)
    return s

def _extract_clean_proa(cell_value):
    """
    Remove fórmulas de HYPERLINK e retorna apenas os dígitos do PROA.
    Ex: '=HYPERLINK("..."; "23/1900-00...")' -> '23190000...'
    """
    val = str(cell_value)
    # Se for fórmula, tenta pegar o texto do segundo argumento
    if val.startswith("="):
        # Tenta quebrar por aspas duplas (o texto geralmente é o último item entre aspas)
        parts = val.split('"')
        if len(parts) >= 4:
            # Pega o penúltimo elemento (geralmente o texto do link)
            val = parts[-2]

    # Retorna apenas números
    return re.sub(r"\D", "", val)

def _parse_br_date(date_str: str):
    """Converte string 'dd/mm/aaaa' para objeto date. Retorna None se falhar."""
    if not isinstance(date_str, str) or not date_str.strip():
        return None
    try:
        # Pega apenas os 10 primeiros caracteres (dd/mm/aaaa)
        clean_str = date_str.strip()[:10]
        return datetime.datetime.strptime(clean_str, "%d/%m/%Y").date()
    except ValueError:
        return None

def _clean_company_name(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s+", " ", s).strip()

    # Validação imediata: Remove datas e lixo comum de OCR/Cabeçalhos
    s_upper = s.upper()
    blacklist = ["MINISTÉRIO", "PREFEITURA", "SECRETARIA", "ESTADO", "GOVERNO", "PROCESSO", "DATA DE", "COORDENADORIA", "TRIBUNAL", "INSCRITA"]
    if len(s) < 3 or len(s) > 85 or any(b in s_upper for b in blacklist) or re.search(r"\d{1,2}\s+de\s+[a-zç]+\s+de\s+\d{4}", s_upper):
        return ""

    # Corte inteligente: Prioriza sufixos empresariais
    # Ex: "Empresa LTDA. A seguir..." -> "EMPRESA LTDA"
    m_suffix = re.search(r"^(.*?\s(?:LTDA|EIRELI|S\.?A|S\/A|EPP|ME|MEI|S\.S))(?=[\s.,;]|$)", s, re.IGNORECASE)
    if m_suffix: return m_suffix.group(1).upper()

    # Fallback: Corte por ponto final de frase (evita pegar texto explicativo)
    if ". " in s: s = s.split(". ")[0]

    # Limpeza final de pontuação (preservando abreviações como J.S.)
    s = s.strip(" ,;-")
    if s.endswith(".") and len(s) > 2 and s[-2] != '.': s = s.rstrip(".")

    return s.upper()

def _flex_regex_escape(s: str) -> str:
    return r"\s+".join(re.escape(part) for part in s.split())

def _slice_after_heading(text: str, heading: str, window: int = 1200) -> str:
    heading_regex = _flex_regex_escape(heading)
    m = re.search(heading_regex, text, flags=re.IGNORECASE)
    if not m: return ""
    start = m.end()
    return text[start:start+window]

# ==========================
# FUNÇÕES DE EXTRAÇÃO ESPECÍFICAS
# ==========================
def get_situacao_processo_web(processo_id: str) -> str:
    base_url = "https://secweb.procergs.com.br/pra-aj4/public/proa_retorno_consulta_publica.xhtml"
    params = {"numeroProcesso": processo_id}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    situacao_padrao = "ERRO: Não encontrado"
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        situacao_label_tag = soup.find('label', string=re.compile(r"Situação:"))
        if situacao_label_tag:
            try:
                parent_td = situacao_label_tag.find_parent('td')
                value_td = parent_td.find_next_sibling('td')
                val = value_td.get_text(strip=True)
                return val if val else situacao_padrao
            except: return "ERRO: Falha no parse do HTML"
        else: return situacao_padrao
    except: return "ERRO: Falha na conexão/HTTP"

def get_numero_contrato(text: str) -> str:
    padrao1 = r"TERMO\s+DE\s+CONTRATO\s+EMERGENCIAL\s+DE\s+OBRAS\s+E\s+SERVI[ÇC]OS\s+DE\s+ENGENHARIA\s*N[º°]?\s*([0-9]{1,4}/[0-9]{4})"
    m = re.search(padrao1, text, flags=re.IGNORECASE)
    if m: return m.group(1).strip()
    padrao2 = r"CONTRATO[^\n]{0,120}?N[º°]?\s*([0-9]{1,4}/[0-9]{4})"
    m2 = re.search(padrao2, text, flags=re.IGNORECASE)
    if m2: return m2.group(1).strip()
    return ""

def get_nome_empresa(text: str) -> str:
    texto_normalizado = _norm_text(text)

    # M1 e M2: Padrões de texto jurídico (Intenção/Contra)
    # Regex ajustado para não travar em pontos de abreviação (J.S.), parando em keywords fortes
    patterns = [
        r"intenç(?:ão|ao)\s+de\s+instaurar\s+procedimento\s+notificat(?:ório|orio)\s+contra\s+(?:a\s+)?empresa\s+(.+?)(?=[,;]|inscrita|CNPJ|sediada|$)",
        r"contra\s+(?:a\s+)?empresa\s*[,;]?\s*(.+?)(?=[,;]|inscrita|CNPJ|sediada|$)"
    ]
    for pat in patterns:
        m = re.search(pat, texto_normalizado, flags=re.IGNORECASE | re.DOTALL)
        if m:
            clean = _clean_company_name(m.group(1))
            if clean: return clean

    # Separação de página para evitar falsos positivos no restante do doc
    pages = texto_normalizado.split('\x0c')
    text_p1 = pages[0] if len(pages) > 1 else texto_normalizado[:3000]

    # M3: Cabeçalho Explícito
    m3 = re.search(r"Empresa\s*:\s*(.+?)(?=\n|Local:|CNPJ|Endereço:|$)", text_p1, flags=re.IGNORECASE)
    if m3:
        clean = _clean_company_name(m3.group(1))
        if clean: return clean

    # M4: Padrão Tipo/CTO (Mesma linha)
    m4 = re.search(r"Tipo\s*:\s*(.+?)\s*-\s*CTO", text_p1, flags=re.IGNORECASE)
    if m4:
        clean = _clean_company_name(m4.group(1))
        if clean: return clean

    return "ERRO AO ENCONTRAR O NOME DA EMPRESA"

def get_cnpj_empresa(text: str) -> str:
    CNPJ_SEDUC = "92941681000100"
    texto_normalizado = _norm_text(text)
    CNPJ_FLEX = r"(\d{2})\s*[\.\-\/]?\s*(\d{3})\s*[\.\-\/]?\s*(\d{3})\s*[\.\-\/]?\s*(\d{4})\s*[\.\-\/]?\s*(\d{2})"
    PREFIXOS = [r"inscrita\s+no\s+minist[ée]rio\s+da\s+fazenda", r"inscri[çc][ãa]o\s+n[ºo]?\s+cnpj", r"cnpj\s*[:\-]?\s*", r"sob\s+o\s+n[ºo]?\s*"]

    bloco = _slice_after_heading(texto_normalizado, "TERMO DE ABERTURA", window=3000) or texto_normalizado
    nome = get_nome_empresa(texto_normalizado)

    # 1. Tenta com ancora do nome
    if nome and "ERRO" not in nome.upper():
        nome_flex = re.escape(nome.strip()).replace(r"\ ", r"\s+")
        pattern = re.compile(rf"empresa\s+{nome_flex}\s*[,;]?\s*(?:{'|'.join(PREFIXOS)})\s*{CNPJ_FLEX}", re.IGNORECASE | re.DOTALL)
        m = pattern.search(bloco)
        if m:
            d = "".join(m.groups())
            if d != CNPJ_SEDUC: return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"

    # 2. Fallback
    for prefixo in PREFIXOS:
        pattern = re.compile(rf"{prefixo}\s*{CNPJ_FLEX}", re.IGNORECASE | re.DOTALL)
        for tb in [bloco, texto_normalizado]:
            for m in pattern.finditer(tb):
                d = "".join(m.groups())
                if d != CNPJ_SEDUC: return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    return "ERRO AO ENCONTRAR O CNPJ"

def get_proa_notificatorio(text: str):
    ms = re.findall(r"\b(\d{2}\/\d{4}-\d{7}-\d)\b", text)
    return ms[0] if ms else ""

def get_proa_mae(text: str, proa_atual: str):
    all_proas = re.findall(r"\b(\d{2}\/\d{4}-\d{7}-\d)\b", text)
    candidates = [p for p in all_proas if p != proa_atual]
    if not candidates: return ""
    # Pega o mais antigo (menor ano)
    candidates.sort(key=lambda p: int(p.split("/")[0]) if p.split("/")[0].isdigit() else 99)
    return candidates[0]

# --- Funções do Expediente ---
def _build_proa_regex(proa_notif: str) -> str:
    parts = re.split(r'[/-]', proa_notif)
    if len(parts) != 4: return re.escape(proa_notif)
    a, b, c, d = [p.strip() for p in parts]
    return rf"{a}\s*/\s*{b}\s*-\s*{c.lstrip('0')}\s*-\s*{d}"

def _find_expediente_page_index(pdf_path: str, proa_notif: str) -> int:
    try:
        doc = fitz.open(pdf_path)
        proa_pat = _build_proa_regex(proa_notif)
        pat_header = re.compile(rf"EXPEDIENTE.*?N[\sº°o\.\-\°]*{proa_pat}", re.IGNORECASE | re.DOTALL)
        pat_frase = re.compile(r"Em\s+an[áa]lise\s+aos\s+autos\s+e\s+considerando\s+as\s+raz[õo]es\s+f[áa]ticas\s+e\s+contratuais", re.IGNORECASE | re.DOTALL)

        for i, page in enumerate(doc):
            txt_norm = _norm_text(page.get_text("text"))
            if pat_header.search(txt_norm): return i
            if pat_frase.search(txt_norm) and proa_notif in txt_norm: return i
        doc.close()
    except: pass
    return -1

def _footer_date_from_page(pdf_path: str, page_index: int) -> str:
    try:
        doc = fitz.open(pdf_path)
        blocks = doc[page_index].get_text("blocks")
        cutoff = doc[page_index].rect.height * 0.85
        cands = []
        for (x0,y0,x1,y1,txt,*_) in blocks:
            if y0 >= cutoff:
                for m in re.finditer(r"(\d{2}/\d{2}/\d{4})", txt):
                    cands.append((x0, y0, m.group(1)))
        doc.close()
        if not cands: return ""
        cands.sort(key=lambda t: (t[0], -t[1]))
        return cands[0][2]
    except: return ""

def get_expediente_text_and_date(pdf_path: str, proa_notif: str) -> tuple:
    idx = _find_expediente_page_index(pdf_path, proa_notif)
    if idx < 0: return "", ""
    doc = fitz.open(pdf_path)
    txt = _norm_text(doc[idx].get_text("text"))
    doc.close()
    dt = _footer_date_from_page(pdf_path, idx)
    return txt, dt

def get_tipo_penalidade(exp_text: str) -> str:
    if re.search(r"\bMULTA\b", exp_text, re.IGNORECASE): return "multa"
    if re.search(r"advert(ê|e)ncia", exp_text, re.IGNORECASE): return "advertencia"
    if re.search(r"n[aã]o\s+aplica(ç|c)[aã]o\s+de\s+penalidade", exp_text, re.IGNORECASE): return "nao aplicacao de penalidade"
    return ERR_MSG_TIPO_PENALIDADE

def get_percentual_multa(exp_text: str) -> str:
    # Lógica original validada (28/Nov)
    words_to_num = {
        'zero': 0, 'um': 1, 'uma': 1, 'dois': 2, 'duas': 2, 'três': 3,
        'quatro': 4, 'cinco': 5, 'seis': 6, 'sete': 7, 'oito': 8, 'nove': 9, 'dez': 10
    }

    # Regex específico que funcionou nos testes
    m_num = re.search(r"(?:aplicando\s+)?multa\s+(?:de\s+)?(\d{1,2})\s*%", exp_text, re.IGNORECASE)
    m_word = re.search(r"%\s*\(\s*([^)]+?)\s+por\s+cento\s*\)", exp_text, re.IGNORECASE)

    if not m_num:
        return ERR_MSG_PERCENTUAL_MULTA

    # Remove zero à esquerda (ex: "05" -> 5)
    num_str = m_num.group(1).lstrip('0') or '0'
    num = int(num_str)

    if m_word:
        word = m_word.group(1).strip().lower()
        num_from_word = words_to_num.get(word)

        if num_from_word is not None:
            # Se houver divergência, a lógica original prioriza o extenso ("mais sensato")
            if num != num_from_word:
                return f"{num_from_word}%"
            return f"{num}%"

    # Validação simples de range (0 a 10)
    return f"{num}%" if 0 <= num <= 10 else ERR_MSG_PERCENTUAL_MULTA


def get_impedimentos(exp_text: str) -> str:
    return "CFIL/RS" if re.search(r"CFIL\/RS", exp_text, re.IGNORECASE) else ""

def get_penalidade_meses(exp_text: str) -> str:
    # Helper interno para normalizar texto (mantido da lógica original)
    def normalize_word(w: str) -> str:
        if not w: return ""
        # Remove acentos e deixa minúsculo
        import unicodedata # Mantendo import aqui caso não tenha no global, ou pode mover pra cima
        return unicodedata.normalize('NFKD', w.lower()).encode('ascii', 'ignore').decode('utf-8').strip()

    words = {
        "um": 1, "uma": 1, "dois": 2, "duas": 2, "tres": 3, "tre": 3,
        "quatro": 4, "cinco": 5, "seis": 6, "sete": 7, "oito": 8, "nove": 9, "dez": 10
    }

    # 1. Padrão Principal (Complexo: contexto de suspensão + parênteses)
    pat = r"(?:CFIL/RS\s*,\s*suspendendo\s+o\s+direito\s+de\s+licitar\s+ou\s+contratar\s+com\s+a\s+Administração\s*(?:,|pelo)?\s*)?(?:prazo\s+de|por)\s*(\d{1,2})?\s*\(\s*([^)]+)\s*\)?\s*meses?"
    m = re.search(pat, exp_text, re.IGNORECASE | re.DOTALL)

    if m:
        num_str = m.group(1)
        word_str = m.group(2)
        num = 0

        if num_str:
            num = int(num_str.lstrip('0') or '0')
            if word_str:
                w = normalize_word(word_str)
                # Verifica conflito Digito vs Extenso
                for k, v in words.items():
                    if re.search(k, w):
                        if num != v: num = v # Prioriza extenso
                        break
        elif word_str:
            w = normalize_word(word_str)
            for k, v in words.items():
                if re.search(k, w):
                    num = v
                    break
            else:
                # Se achou texto mas não reconheceu o número, falha
                pass # Vai cair nos fallbacks ou retornar erro no final

        if num > 0:
            return "1 mês" if num == 1 else f"{num} meses"

    # 2. Fallbacks (Lógica sequencial original - não mexi na ordem)

    # Fallback 1: "prazo de 12 mes"
    m1 = re.search(r"prazo\s+de\s+\(?(\d{1,2})\)?\s+mes", exp_text, re.IGNORECASE | re.DOTALL)
    if m1:
        v = int(m1.group(1).lstrip('0') or '0')
        return "1 mês" if v == 1 else f"{v} meses"

    # Fallback 2: "prazo de (doze) mes"
    m2 = re.search(r"prazo\s+de\s+\(([^)]+)\)\s+mes", exp_text, re.IGNORECASE | re.DOTALL)
    if m2:
        w = normalize_word(m2.group(1))
        for k, v in words.items():
            if re.search(k, w):
                return "1 mês" if v == 1 else f"{v} meses"

    # Fallback 3: "prazo de doze mes" (sem parênteses)
    m3 = re.search(r"prazo\s+de\s+([a-zçãõéê]+)\s+mes", exp_text, re.IGNORECASE | re.DOTALL)
    if m3:
        w = normalize_word(m3.group(1))
        for k, v in words.items():
            if re.fullmatch(k, w):
                return "1 mês" if v == 1 else f"{v} meses"

    return ERR_MSG_PENALIDADE_MESES

def get_ultima_atualizacao_processo(pdf_path):
    # Tenta pegar do rodapé da última página
    try:
        doc = fitz.open(pdf_path)
        dt = _footer_date_from_page(pdf_path, len(doc)-1)
        doc.close()
        if dt: return dt
    except: pass
    return ""

def get_data_analise_agora():
    return datetime.datetime.now().strftime("%d/%m/%Y")

def aplicar_regras_status(data: dict) -> dict:
    tipo = data.get("tipo_penalidade", "").lower()
    if tipo in ["advertencia", "nao aplicacao de penalidade", ERR_MSG_STATUS]:
        data["percentual_multa"] = ""
        data["divida_ativa"] = ""
        data["penalidade_meses"] = ""
    return data

# ==========================
# EXTRAÇÃO DE CAMPOS (CORRIGIDA)
# ==========================
def extract_fields_from_pdf(pdf_path: str) -> dict:
    full_text = extract_pdf_text(pdf_path)
    proa_notif = get_proa_notificatorio(full_text)
    cnpj_empresa = get_cnpj_empresa(full_text)

    # Status Web
    status_proa = ""
    if proa_notif:
        num = re.sub(r"\D", "", proa_notif)
        if num:
            print(f"🔎 Consultando status do PROA {num}...")
            time.sleep(1)
            status_proa = get_situacao_processo_web(num) or ""
            print(f"→ Status: {status_proa}")

    # Expediente
    exp_text, quando_aplicada = ("", "")
    if proa_notif:
        exp_text, quando_aplicada = get_expediente_text_and_date(pdf_path, proa_notif)

    if exp_text:
        tipo = get_tipo_penalidade(exp_text)
        perc = get_percentual_multa(exp_text)
        imp = get_impedimentos(exp_text)
        meses = get_penalidade_meses(exp_text)
    else:
        tipo, perc, imp, meses = ERR_MSG_TIPO_PENALIDADE, ERR_MSG_PERCENTUAL_MULTA, ERR_MSG_IMPEDIMENTOS, ERR_MSG_PENALIDADE_MESES

    data = {
        "numero_contrato": get_numero_contrato(full_text),
        "nome_empresa": get_nome_empresa(full_text),
        "cnpj_empresa": cnpj_empresa,
        "proa_notificatorio": proa_notif,
        "proa_mae": get_proa_mae(full_text, proa_notif),
        "status_processo": status_proa,
        "valor_contrato_consolidado": "",
        "tipo_penalidade": tipo,
        "percentual_multa": perc,
        "valor_multa": "",
        "impedimentos": imp,
        "penalidade_meses": meses,
        "data_penalizacao": quando_aplicada,
        "ultima_analise_feita": get_data_analise_agora(),
        "ultima_atualizacao_processo": get_ultima_atualizacao_processo(pdf_path),
    }

    data = aplicar_regras_status(data)
    for col in COLUMNS: data.setdefault(col, "")

    return data  # <--- O IMPORTANTE QUE ESTAVA FALTANDO

# ==========================
# FUNÇÕES DE PLANILHA E DRIVE (CORRIGIDAS)
# ==========================
def load_or_create_gsheet(gc, sheet_name, worksheet_name, columns):
    try: sh = gc.open(sheet_name)
    except: raise Exception("Planilha não encontrada.")
    try: ws = sh.worksheet(worksheet_name)
    except:
        ws = sh.add_worksheet(title=worksheet_name, rows=1, cols=len(columns))
        ws.update([columns])
        return pd.DataFrame(columns=columns), ws

    df = get_as_dataframe(ws, dtype=str)
    for col in columns:
        if col not in df.columns: df[col] = pd.NA
    df = df.fillna("")[columns]
    return df, ws

def upsert_row(df: pd.DataFrame, row: dict) -> pd.DataFrame:
    key_raw = row.get("proa_notificatorio", "")
    # A chave de busca é puramente numérica
    key_clean = re.sub(r"\D", "", str(key_raw))

    if not key_clean:
        return df

    # Cria uma série temporária só com os números da coluna PROA do DF atual
    # Isso garante que vamos achar o processo mesmo se ele estiver como Link
    df_proas_clean = df["proa_notificatorio"].apply(_extract_clean_proa)

    if key_clean in df_proas_clean.values:
        # Pega o índice da primeira ocorrência
        idx = df_proas_clean[df_proas_clean == key_clean].index[0]

        # Atualiza as colunas (mantendo o link antigo se não quisermos forçar reescrita,
        # mas aqui vamos sobrescrever os dados novos)
        for col in COLUMNS:
            # Só atualiza se o dado novo não for vazio, ou força atualização
            val = row.get(col, "")
            df.at[idx, col] = val
    else:
        # Se não achou, aí sim cria nova linha
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    return df

def _map_pdf_links_in_folder(drive, folder_id):
    """Mapeia PDFs com paginação para evitar erro 400."""
    mapping = {}
    page_token = None
    print(f"📂 Mapeando Drive ID: {folder_id}...")
    while True:
        try:
            res = drive.files().list(
                q=f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false",
                fields="nextPageToken, files(id, name, webViewLink)",
                pageSize=1000, pageToken=page_token
            ).execute()
            for f in res.get("files", []):
                link = f.get("webViewLink") or f"https://drive.google.com/file/d/{f['id']}/view"
                mapping[f["name"]] = link
            page_token = res.get('nextPageToken')
            if not page_token: break
        except Exception as e:
            print(f"Erro no mapeamento: {e}")
            break
    print(f"✅ Arquivos mapeados: {len(mapping)}")
    return mapping

def apply_drive_links(df, name_to_link):
    """Aplica Hyperlinks formato PT-BR (;) nas colunas PROA e Nome."""
    df_out = df.copy()
    if "proa_notificatorio" not in df_out.columns: return df_out

    # Prepara mapeamento limpo (apenas numeros)
    map_clean = {}
    for name, link in name_to_link.items():
        digits = re.sub(r"\D", "", name)
        if digits: map_clean[digits] = link

    for i, row in df_out.iterrows():
        proa = str(row.get("proa_notificatorio", "")).strip()
        nome = str(row.get("nome_empresa", "")).strip()

        # Removemos a limpeza aqui dentro para usar a que fizemos lá fora ou a crua
        # Mas para garantir o match do link:
        proa_digits = re.sub(r"\D", "", proa)

        # Tenta achar link
        link = ""
        if proa_digits:
            if proa_digits in map_clean:
                link = map_clean[proa_digits]
            else:
                # Tentativa de match parcial (tail)
                for w in [14, 12, 10]:
                    if len(proa_digits) >= w and proa_digits[-w:] in map_clean:
                         link = map_clean[proa_digits[-w:]] # Lógica simplificada
                         break

        if link:
             safe_proa = proa.replace('"', "'")
             safe_nome = nome.replace('"', "'")

             # Se já for fórmula, não aplica de novo para não quebrar
             if not safe_proa.startswith("=HYPERLINK"):
                 df_out.at[i, "proa_notificatorio"] = f'=HYPERLINK("{link}"; "{safe_proa}")'

             if not safe_nome.startswith("=HYPERLINK"):
                 df_out.at[i, "nome_empresa"] = f'=HYPERLINK("{link}"; "{safe_nome}")'

             # REMOVIDO: df_out.at[i, "link_pdf"] = link

    return df_out

# ==========================
# PIPELINE PRINCIPAL
# ==========================
# ==========================
# PIPELINE PRINCIPAL (LÓGICA BLINDADA)
# ==========================
def process_all_pdfs(gc, pdf_dir=PDF_DIR, force_update=False):
# 1. Carrega Planilha
    df, ws = load_or_create_gsheet(gc, GSHEET_NAME, GSHEET_WORKSHEET_NAME, COLUMNS)

    # 2. Mapeia Links do Drive
    name_to_link = _map_pdf_links_in_folder(drive, FOLDER_ID_DRIVE)

    # 3. Cria Mapa de Datas Existentes (CORRIGIDO PARA LER DENTRO DO HYPERLINK)
    existing_dates = {}
    if not df.empty and "proa_notificatorio" in df.columns:
        for _, row in df.iterrows():
            # Usa a função nova para ignorar o =HYPERLINK e pegar só o número
            clean_proa = _extract_clean_proa(row["proa_notificatorio"])

            d_str = str(row.get("ultima_atualizacao_processo", ""))
            d_obj = _parse_br_date(d_str)

            if clean_proa and d_obj:
                existing_dates[clean_proa] = d_obj

    print(f"📊 Processos reconhecidos na planilha: {len(existing_dates)}")

    # 4. Processa PDFs
    for fname in os.listdir(pdf_dir):
        if not fname.lower().endswith(".pdf"): continue
        pdf_path = os.path.join(pdf_dir, fname)

        should_process = True

        # Tenta extrair números do nome do arquivo para comparar com a planilha
        # Ex: "Processo_241900.pdf" -> "241900"
        proa_digits_pdf = re.sub(r"\D", "", fname)

        # --- LÓGICA DE DECISÃO ---
        if not force_update:
            # CASO 1: Arquivo SEM números no nome (não dá pra saber quem é sem ler) -> Processa
            if not proa_digits_pdf:
                should_process = True

            # CASO 2: O processo JÁ ESTÁ na planilha -> Verifica a data
            elif proa_digits_pdf in existing_dates:
                data_pdf_str = get_ultima_atualizacao_processo(pdf_path)
                data_pdf_obj = _parse_br_date(data_pdf_str)
                data_planilha = existing_dates[proa_digits_pdf]

                # Se conseguiu ler a data do PDF e ela é igual ou menor que a da planilha
                if data_pdf_obj and data_pdf_obj <= data_planilha:
                    print(f"⏩ Pulando {fname} (Já atualizado em {data_pdf_str})")
                    should_process = False
                else:
                    print(f"🔄 Atualizando {fname} (Nova data encontrada)")
                    should_process = True

            # CASO 3: O processo NÃO ESTÁ na planilha (Seu caso de teste!)
            else:
                print(f"🆕 Novo: {fname} -> Processando...")
                should_process = True

        if not should_process:
            continue

        # --- EXTRAÇÃO E SALVAMENTO ---
        print(f"   📂 Lendo PDF: {fname}...")
        try:
            row = extract_fields_from_pdf(pdf_path)

            if row is None:
                print("   ⚠️ Falha na extração. Pulando.")
                continue

            row["link_pdf"] = name_to_link.get(fname, "")
            df = upsert_row(df, row)

        except Exception as e:
            print(f"   ❌ Erro: {e}")
            continue

    # 5. Finalização
    df = df[df["proa_notificatorio"].notna() & (df["proa_notificatorio"].str.strip() != "")].copy()

    # Aplica hyperlinks
    df_write = apply_drive_links(df, name_to_link)

    print("Atualizando planilha...")
    ws.clear()
    set_with_dataframe(ws, df_write, include_index=False, resize=True)
    print("Sucesso! ✅")
    return df