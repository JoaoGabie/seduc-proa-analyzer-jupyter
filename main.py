# ==============================================================================
# 🚀 EXECUÇÃO E DASHBOARD DE MONITORAMENTO
# ==============================================================================
from IPython.display import display, Markdown, HTML
import pandas as pd

# Função para estilizar status com cores
def style_status(val):
    """Colore a célula dependendo do texto do Status"""
    val_str = str(val).lower()
    if 'ativo' in val_str:
        return 'background-color: #d4edda; color: #155724; font-weight: bold;' # Verde Suave
    elif 'erro' in val_str or 'falha' in val_str:
        return 'background-color: #f8d7da; color: #721c24; font-weight: bold;' # Vermelho Suave
    elif 'arquivado' in val_str or 'encerrado' in val_str:
        return 'background-color: #e2e3e5; color: #383d41;' # Cinza
    elif val_str == '' or val_str == 'nan':
        return 'background-color: #fff3cd; color: #856404;' # Amarelo (Alerta)
    return ''

try:
    # 1. Executa o Pipeline
    display(Markdown("### ⚙️ Iniciando Processamento..."))
    # force_update=True lê tudo / False lê só novos e atualizados
    df_resultado = process_all_pdfs(gc, force_update=False)

    # ----------------------------
    # DASHBOARD
    # ----------------------------
    display(Markdown("---"))
    display(Markdown("# 📊 Painel de Controle: Processo Notificatório"))

    if df_resultado.empty:
        display(Markdown("### ⚠️ Nenhum dado foi processado ou a planilha está vazia."))
    else:
        # A. KPIs (Indicadores Principais)
        total_docs = len(df_resultado)
        total_ativos = df_resultado['status_processo'].astype(str).str.contains('Ativo', case=False).sum()
        total_erros = df_resultado['status_processo'].astype(str).str.contains('ERRO', case=False).sum()

        kpi_html = f"""
        <div style="display: flex; gap: 20px; margin-bottom: 20px;">
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #ddd; flex: 1; text-align: center;">
                <h2 style="margin:0; color: #007bff;">{total_docs}</h2>
                <p style="margin:0; color: #666;">Processos Totais</p>
            </div>
            <div style="background-color: #d4edda; padding: 15px; border-radius: 10px; border: 1px solid #c3e6cb; flex: 1; text-align: center;">
                <h2 style="margin:0; color: #155724;">{total_ativos}</h2>
                <p style="margin:0; color: #155724;">Ativos (Web)</p>
            </div>
            <div style="background-color: #f8d7da; padding: 15px; border-radius: 10px; border: 1px solid #f5c6cb; flex: 1; text-align: center;">
                <h2 style="margin:0; color: #721c24;">{total_erros}</h2>
                <p style="margin:0; color: #721c24;">Com Erro/Falha</p>
            </div>
        </div>
        """
        display(HTML(kpi_html))

        # B. Tabela Detalhada (Estilizada)
        display(Markdown("### 📋 Status Detalhado por Processo"))

        cols_status = [
            "proa_notificatorio", "status_processo", "nome_empresa",
            "ultima_atualizacao_processo", "tipo_penalidade",
            "percentual_multa", "penalidade_meses", "divida_ativa"
        ]

        # Garante colunas
        for c in cols_status:
            if c not in df_resultado.columns: df_resultado[c] = ""

        df_view = (df_resultado[cols_status].copy()
                   .sort_values(by=["status_processo", "proa_notificatorio"], na_position="last")
                   .reset_index(drop=True))

        # Aplica estilo (cores condicionais)
        styled_table = (df_view.style
            .map(style_status, subset=['status_processo'])
            .set_properties(**{'text-align': 'left'})
            .set_table_styles([{'selector': 'th', 'props': [('text-align', 'left'), ('background-color', '#f1f1f1')]}])
        )
        display(styled_table)

        # C. Distribuição e Alertas
        display(Markdown("### 📈 Distribuição & Alertas"))

        # Cria dataframe de contagem
        contagem = df_resultado["status_processo"].fillna("Sem Status").replace("", "Sem Status").value_counts().to_frame("Qtd")

        # Barra de dados simples dentro da tabela
        display(contagem.style.bar(subset=['Qtd'], color='#5fba7d'))

        # D. Alerta de Vazios
        sem_status = df_resultado[df_resultado["status_processo"].fillna("") == ""]
        if not sem_status.empty:
            display(Markdown(f"### ⚠️ ATENÇÃO: {len(sem_status)} Processos sem retorno de status"))
            display(sem_status[["proa_notificatorio", "nome_empresa"]].style.hide(axis="index"))
        else:
            display(Markdown("✅ **Sucesso:** Todos os processos possuem status definido."))

except NameError:
    display(Markdown("## ❌ ERRO CRÍTICO: Variável `gc` não encontrada"))
    display(Markdown("Por favor, rode a célula de **Autenticação** no topo do notebook."))
except Exception as e:
    display(Markdown(f"## ❌ Ocorreu um erro inesperado"))
    print(e)