import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
from msal import ConfidentialClientApplication
import unicodedata

# === CREDENCIAIS via st.secrets ===
CLIENT_ID = st.secrets["CLIENT_ID"]
CLIENT_SECRET = st.secrets["CLIENT_SECRET"]
TENANT_ID = st.secrets["TENANT_ID"]
EMAIL_ONEDRIVE = st.secrets["EMAIL_ONEDRIVE"]
PASTA = "Documentos Compartilhados/LimparAuto/FontedeDados"

# === AUTENTICAÇÃO ===
def obter_token():
    """Obtém token de acesso para Microsoft Graph API"""
    try:
        app = ConfidentialClientApplication(
            CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{TENANT_ID}",
            client_credential=CLIENT_SECRET
        )
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        token = result.get("access_token")
        if not token:
            st.error("❌ Falha na autenticação - Token não obtido")
        return token
    except Exception as e:
        st.error(f"❌ Erro na autenticação: {str(e)}")
        return None

# === UPLOAD E BACKUP ===
def mover_arquivo_existente(nome_arquivo, token):
    """Move arquivo existente para backup antes de substituir"""
    try:
        url = f"https://graph.microsoft.com/v1.0/sites/{st.secrets['SITE_ID']}/drives/{st.secrets['DRIVE_ID']}/root:/{PASTA}/{nome_arquivo}"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            file_id = response.json().get("id")
            timestamp = datetime.now().strftime("%Y-%m-%d_%Hh%M")
            novo_nome = nome_arquivo.replace(".xlsx", f"_backup_{timestamp}.xlsx")
            
            patch_url = f"https://graph.microsoft.com/v1.0/sites/{st.secrets['SITE_ID']}/drives/{st.secrets['DRIVE_ID']}/items/{file_id}"
            patch_body = {"name": novo_nome}
            patch_headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            patch_response = requests.patch(patch_url, headers=patch_headers, json=patch_body)
            
            if patch_response.status_code not in [200, 201]:
                st.warning(f"⚠️ Aviso: Não foi possível criar backup do arquivo existente")
                
    except Exception as e:
        st.warning(f"⚠️ Erro ao processar backup: {str(e)}")

def upload_onedrive(nome_arquivo, conteudo_arquivo, token):
    """Faz upload de arquivo para OneDrive"""
    try:
        mover_arquivo_existente(nome_arquivo, token)
        
        url = f"https://graph.microsoft.com/v1.0/sites/{st.secrets['SITE_ID']}/drives/{st.secrets['DRIVE_ID']}/root:/{PASTA}/{nome_arquivo}:/content"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream"
        }
        response = requests.put(url, headers=headers, data=conteudo_arquivo)
        return response.status_code in [200, 201], response.status_code, response.text
        
    except Exception as e:
        return False, 500, f"Erro interno: {str(e)}"



# === FUNÇÕES DE CONSOLIDAÇÃO ===
def validar_dados_enviados(df, responsavel):
    """Valida os dados enviados pelo usuário"""
    erros = []
    
    # Validar responsável
    if not responsavel or not responsavel.strip():
        erros.append("⚠️ O nome do responsável é obrigatório")
    
    # Validar se existe coluna DATA
    if "DATA" not in df.columns:
        erros.append("⚠️ A planilha deve conter uma coluna 'DATA'")
    else:
        # Validar se as datas são válidas
        df_temp = df.copy()
        df_temp["DATA"] = pd.to_datetime(df_temp["DATA"], errors="coerce")
        datas_validas = df_temp["DATA"].notna().sum()
        
        if datas_validas == 0:
            erros.append("⚠️ Nenhuma data válida encontrada na coluna 'DATA'")
        elif datas_validas < len(df):
            erros.append(f"⚠️ {len(df) - datas_validas} linhas com datas inválidas serão ignoradas")
    
    return erros

def processar_consolidacao(df_novo, responsavel, token):
    """Processa a consolidação dos dados - Atualiza ou insere linha por linha"""
    consolidado_nome = "Reports_Geral_Consolidado.xlsx"

    # 1. Baixar arquivo consolidado existente
    url = f"https://graph.microsoft.com/v1.0/sites/{st.secrets['SITE_ID']}/drives/{st.secrets['DRIVE_ID']}/root:/{PASTA}/{consolidado_nome}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)

    if r.status_code == 200:
        try:
            df_consolidado = pd.read_excel(BytesIO(r.content))
            df_consolidado.columns = df_consolidado.columns.str.strip().str.upper()
            st.info("📂 Arquivo consolidado existente carregado")
        except Exception as e:
            st.warning(f"⚠️ Erro ao ler arquivo consolidado existente: {e}")
            df_consolidado = pd.DataFrame()
    else:
        df_consolidado = pd.DataFrame()
        st.info("📂 Criando novo arquivo consolidado")

    # 2. Preparar dados novos
    df_novo = df_novo.copy()
    df_novo["RESPONSÁVEL"] = responsavel.strip()
    df_novo.columns = df_novo.columns.str.strip().str.upper()
    df_novo["DATA"] = pd.to_datetime(df_novo["DATA"], errors="coerce")
    df_novo = df_novo.dropna(subset=["DATA"])

    if df_novo.empty:
        st.error("❌ Nenhum registro válido para consolidar")
        return False

    # 3. Consolidar linha por linha (comparação completa)
    if not df_consolidado.empty:
        df_consolidado["DATA"] = pd.to_datetime(df_consolidado["DATA"], errors="coerce")
        df_consolidado = df_consolidado.dropna(subset=["DATA"])

        registros_atualizados = 0
        registros_inseridos = 0
        colunas = df_novo.columns.tolist()

        for _, row_nova in df_novo.iterrows():
            cond = (
                (df_consolidado["DATA"].dt.normalize() == row_nova["DATA"].normalize()) &
                (df_consolidado["RESPONSÁVEL"].str.strip() == row_nova["RESPONSÁVEL"].strip())
            )
            possiveis = df_consolidado[cond]

            igual_exata = (possiveis[colunas] == row_nova[colunas].values).all(axis=1)
            if igual_exata.any():
                continue  # já existe linha idêntica → ignorar

            if not possiveis.empty:
                index = possiveis.index[0]
                df_consolidado.loc[index, colunas] = row_nova.values
                registros_atualizados += 1
            else:
                df_consolidado = pd.concat([df_consolidado, pd.DataFrame([row_nova])], ignore_index=True)
                registros_inseridos += 1

        df_final = df_consolidado.copy()
        st.info(f"🔁 {registros_atualizados} registros atualizados")
        st.info(f"➕ {registros_inseridos} registros inseridos")
    else:
        df_final = df_novo.copy()
        st.info("📂 Primeiro envio - criando arquivo consolidado")

    # 4. Ordenar e salvar
    df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"]).reset_index(drop=True)
    buffer = BytesIO()
    df_final.to_excel(buffer, index=False, sheet_name="Dados")
    buffer.seek(0)

    # 5. Backup e envio
    salvar_arquivo_enviado(df_novo, responsavel, token)
    sucesso, status, resposta = upload_onedrive(consolidado_nome, buffer.read(), token)

    if sucesso:
        st.success("✅ Consolidação realizada com sucesso!")
        st.info(f"📊 Total de registros no consolidado: {len(df_final)}")
        st.info(f"📊 Registros inseridos: {registros_inseridos}")
        st.info(f"📊 Registros atualizados: {registros_atualizados}")
        data_min = df_novo["DATA"].min().strftime("%d/%m/%Y")
        data_max = df_novo["DATA"].max().strftime("%d/%m/%Y")
        st.info(f"📅 Período processado: {data_min} até {data_max}")
        return True
    else:
        st.error(f"❌ Erro no upload: {status}")
        st.code(resposta)
        return False

def salvar_arquivo_enviado(df, responsavel, token):
    """Salva uma cópia do arquivo enviado pelo responsável"""
    try:
        if not df.empty and "DATA" in df.columns:
            data_base = df["DATA"].min()
            nome_pasta = f"Relatorios_Enviados/{data_base.strftime('%Y-%m')}"
            timestamp = datetime.now().strftime('%d-%m-%Y_%Hh%M')
            nome_arquivo = f"{nome_pasta}/{responsavel.strip()}_{timestamp}.xlsx"
            
            buffer_envio = BytesIO()
            df.to_excel(buffer_envio, index=False, sheet_name="Dados")
            buffer_envio.seek(0)
            
            sucesso, _, _ = upload_onedrive(nome_arquivo, buffer_envio.read(), token)
            if sucesso:
                st.info(f"💾 Cópia salva em: {nome_arquivo}")
    except Exception as e:
        st.warning(f"⚠️ Não foi possível salvar cópia do arquivo: {e}")

# === INTERFACE STREAMLIT ===
st.set_page_config(page_title="Upload e Gestão de Planilhas", layout="wide")

st.markdown(
    '''
    <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 20px;">
        <img src="logo_horizontal.png" width="180"/>
        <h2 style="margin: 0; color: #2E8B57;">DSView BI – Upload de Planilhas</h2>
    </div>
    ''',
    unsafe_allow_html=True
)

# Sidebar navigation
st.sidebar.markdown("### 📤 Upload de Planilhas")
st.sidebar.markdown("Sistema de consolidação de relatórios")

token = obter_token()

# Verificar se o token foi obtido com sucesso
if not token:
    st.error("❌ Não foi possível autenticar. Verifique as credenciais.")
    st.stop()

st.markdown("## 📤 Upload de Planilha Excel")
st.divider()

uploaded_file = st.file_uploader("Escolha um arquivo Excel", type=["xlsx"])

# Campo obrigatório para responsável
responsavel = st.text_input(
    "Digite seu nome (responsável): *", 
    placeholder="Ex: João Silva",
    help="Este campo é obrigatório"
)

if uploaded_file:
    try:
        xls = pd.ExcelFile(uploaded_file)
        sheets = xls.sheet_names
        sheet = st.selectbox("Selecione a aba:", sheets) if len(sheets) > 1 else sheets[0]
        df = pd.read_excel(uploaded_file, sheet_name=sheet)
        df.columns = df.columns.str.strip().str.upper()
        
        st.success(f"✅ Arquivo carregado: {uploaded_file.name}")
    except Exception as e:
        st.error(f"❌ Erro ao ler o Excel: {e}")
        df = None

    if df is not None:
        st.subheader("👀 Prévia dos dados")
        st.dataframe(df.head(5), use_container_width=True, height=200)

        st.subheader("📊 Resumo dos dados")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Linhas", df.shape[0])
        with col2:
            st.metric("Colunas", df.shape[1])

        # Verificar colunas com valores nulos
        colunas_nulas = df.columns[df.isnull().any()].tolist()
        if colunas_nulas:
            st.warning(f"⚠️ Colunas com valores nulos: {', '.join(colunas_nulas)}")
        else:
            st.success("✅ Nenhuma coluna com valores nulos.")

        # Validações antes do envio
        st.subheader("🔍 Validações")
        erros = validar_dados_enviados(df, responsavel)
        
        if erros:
            for erro in erros:
                st.error(erro)
        else:
            st.success("✅ Todos os dados estão válidos para consolidação")

        # Botão de envio
        if st.button("📧 Consolidar Dados", type="primary", disabled=bool(erros)):
            if erros:
                st.error("❌ Corrija os erros acima antes de prosseguir")
            else:
                with st.spinner("🔄 Processando consolidação..."):
                    sucesso = processar_consolidacao(df, responsavel, token)
                    if sucesso:
                        st.balloons()

# Rodapé com informações
st.divider()
st.markdown(
    """
    <div style="text-align: center; color: #666; font-size: 0.8em;">
        DSView BI - Sistema de Consolidação de Relatórios<br>
        ⚠️ Certifique-se de que sua planilha contenha a coluna 'DATA' e informe o responsável
    </div>
    """,
    unsafe_allow_html=True
)