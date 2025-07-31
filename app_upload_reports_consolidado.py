import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from msal import ConfidentialClientApplication
from io import BytesIO

# === CREDENCIAIS via st.secrets ===
CLIENT_ID = st.secrets["CLIENT_ID"]
CLIENT_SECRET = st.secrets["CLIENT_SECRET"]
TENANT_ID = st.secrets["TENANT_ID"]
EMAIL_ONEDRIVE = st.secrets["EMAIL_ONEDRIVE"]
PASTA = "Documentos Compartilhados/LimparAuto/FontedeDados"

# === AUTENTICAÇÃO ===
def obter_token():
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/" + TENANT_ID,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    return result.get("access_token")

# === GERENCIAMENTO DE ARQUIVOS ===
def listar_arquivos(token):
    url = f"https://graph.microsoft.com/v1.0/users/{EMAIL_ONEDRIVE}/drive/root:/{PASTA}:/children"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return r.json().get("value", [])
    else:
        st.error(f"Erro ao listar: {r.status_code}")
        st.code(r.text)
        return []

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

aba = st.sidebar.radio("📂 Navegar", ["📤 Upload de planilha", "📁 Gerenciar arquivos"])

token = obter_token()

if aba == "📤 Upload de planilha":
    st.info("Funcionalidade de upload ainda não implementada neste arquivo.")

elif aba == "📁 Gerenciar arquivos":
    st.markdown("## 📂 Painel de Arquivos")
    st.divider()
    if token:
        arquivos = listar_arquivos(token)
        if arquivos:
            for arq in arquivos:
                with st.expander(f"📄 {arq['name']}"):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.markdown(f"[🔗 Acessar arquivo]({arq['@microsoft.graph.downloadUrl']})")
                        st.write(f"Tamanho: {round(arq['size']/1024, 2)} KB")
        else:
            st.info("Nenhum arquivo encontrado na pasta uploads.")
    else:
        st.error("Erro ao autenticar.")