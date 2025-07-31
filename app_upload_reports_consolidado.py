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
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    return result.get("access_token")

# === UPLOAD E BACKUP ===
def mover_arquivo_existente(nome_arquivo, token):
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
        requests.patch(patch_url, headers=patch_headers, json=patch_body)

def upload_onedrive(nome_arquivo, conteudo_arquivo, token):
    mover_arquivo_existente(nome_arquivo, token)
    url = f"https://graph.microsoft.com/v1.0/sites/{st.secrets['SITE_ID']}/drives/{st.secrets['DRIVE_ID']}/root:/{PASTA}/{nome_arquivo}:/content"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }
    response = requests.put(url, headers=headers, data=conteudo_arquivo)
    return response.status_code in [200, 201], response.status_code, response.text

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
    st.markdown("## 📤 Upload de Planilha Excel")
    st.divider()

    uploaded_file = st.file_uploader("Escolha um arquivo Excel", type=["xlsx"])
    responsavel = st.text_input("Digite seu nome (responsável):")

    if uploaded_file:
        try:
            xls = pd.ExcelFile(uploaded_file)
            sheets = xls.sheet_names
            sheet = st.selectbox("Selecione a aba:", sheets) if len(sheets) > 1 else sheets[0]
            df = pd.read_excel(uploaded_file, sheet_name=sheet)
            df.columns = df.columns.str.strip().str.upper()
        except Exception as e:
            st.error(f"Erro ao ler o Excel: {e}")
            df = None

        if df is not None:
            st.dataframe(df.head(5), use_container_width=True, height=200)

            st.subheader("📊 Resumo dos dados")
            st.write(f"📏 Linhas: {df.shape[0]} | Colunas: {df.shape[1]}")

            colunas_nulas = df.columns[df.isnull().any()].tolist()
            if colunas_nulas:
                st.warning(f"⚠️ Colunas com valores nulos: {', '.join(colunas_nulas)}")
            else:
                st.success("✅ Nenhuma coluna com valores nulos.")

            

            if st.button("📧 Enviar e Consolidar"):
                if not responsavel.strip():
                    st.warning("⚠️ Informe o nome do responsável.")
                else:
                    with st.spinner("Consolidando e atualizando..."):
                        consolidado_nome = "Reports_Geral_Consolidado.xlsx"
                        url = f"https://graph.microsoft.com/v1.0/sites/{st.secrets['SITE_ID']}/drives/{st.secrets['DRIVE_ID']}/root:/{PASTA}/{consolidado_nome}:/content"
                        headers = {"Authorization": f"Bearer {token}"}
                        r = requests.get(url, headers=headers)
                        if r.status_code == 200:
                            df_consolidado = pd.read_excel(BytesIO(r.content))
                            df_consolidado.columns = df_consolidado.columns.str.strip().str.upper()
                        else:
                            df_consolidado = pd.DataFrame()

                        df["RESPONSÁVEL"] = responsavel.strip()

                        df.columns = df.columns.str.strip().str.upper()
                        df_consolidado.columns = df_consolidado.columns.str.strip().str.upper()

                        if "DATA" not in df.columns or "DATA" not in df_consolidado.columns:
                            st.error("❌ A planilha enviada e o consolidado precisam conter a coluna 'DATA'.")
                        else:
                            df["DATA"] = pd.to_datetime(df["DATA"])
                            df_consolidado["DATA"] = pd.to_datetime(df_consolidado["DATA"])
                            datas_novas = df["DATA"].dt.normalize().unique()
                            df_consolidado = df_consolidado[
                                ~(
                                    (df_consolidado["RESPONSÁVEL"] == responsavel.strip()) &
                                    (df_consolidado["DATA"].dt.normalize().isin(datas_novas))
                                )
                            ]
                            df_final = pd.concat([df_consolidado, df], ignore_index=True)
                            buffer = BytesIO()
                            df_final.to_excel(buffer, index=False, sheet_name="Dados")
                            buffer.seek(0)
                            
                            # Salvar planilha enviada pelo responsável
                            try:
                                data_base = df["DATA"].min()
                                nome_pasta = f"Relatorios_Enviados/{data_base.strftime('%Y-%m')}"
                                nome_arquivo = f"{nome_pasta}/{responsavel.strip()}_{datetime.now().strftime('%d-%m-%Y_%Hh%M')}.xlsx"
                                buffer_envio = BytesIO()
                                df.to_excel(buffer_envio, index=False)
                                buffer_envio.seek(0)
                                upload_onedrive(nome_arquivo, buffer_envio.read(), token)
                            except Exception as e:
                                st.warning(f"⚠️ Não foi possível salvar o arquivo enviado: {e}")

                            sucesso, status, resposta = upload_onedrive(consolidado_nome, buffer.read(), token)
                            if sucesso:
                                st.success("✅ Consolidado atualizado com sucesso!")
                            else:
                                st.error(f"❌ Erro {status}")
                                st.code(resposta)

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