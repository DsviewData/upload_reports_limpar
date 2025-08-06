import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
from msal import ConfidentialClientApplication
import unicodedata
import logging

# Configurar logging para debug
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === CREDENCIAIS via st.secrets ===
try:
    CLIENT_ID = st.secrets["CLIENT_ID"]
    CLIENT_SECRET = st.secrets["CLIENT_SECRET"]
    TENANT_ID = st.secrets["TENANT_ID"]
    EMAIL_ONEDRIVE = st.secrets["EMAIL_ONEDRIVE"]
    SITE_ID = st.secrets["SITE_ID"]
    DRIVE_ID = st.secrets["DRIVE_ID"]
except KeyError as e:
    st.error(f"❌ Credencial não encontrada: {e}")
    st.stop()

PASTA = "Documentos Compartilhados/LimparAuto/FontedeDados"

# === AUTENTICAÇÃO ===
@st.cache_data(ttl=3300)  # Cache por 55 minutos (token válido por 1h)
def obter_token():
    """Obtém token de acesso para Microsoft Graph API"""
    try:
        app = ConfidentialClientApplication(
            CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{TENANT_ID}",
            client_credential=CLIENT_SECRET
        )
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        
        if "access_token" not in result:
            error_desc = result.get("error_description", "Token não obtido")
            st.error(f"❌ Falha na autenticação: {error_desc}")
            return None
            
        return result["access_token"]
        
    except Exception as e:
        st.error(f"❌ Erro na autenticação: {str(e)}")
        logger.error(f"Erro de autenticação: {e}")
        return None

# === FUNÇÕES AUXILIARES ===
def criar_pasta_se_nao_existir(caminho_pasta, token):
    """Cria pasta no OneDrive se não existir"""
    try:
        # Dividir o caminho em partes
        partes = caminho_pasta.split('/')
        caminho_atual = ""
        
        for parte in partes:
            if not parte:
                continue
                
            caminho_anterior = caminho_atual
            caminho_atual = f"{caminho_atual}/{parte}" if caminho_atual else parte
            
            # Verificar se a pasta existe
            url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{caminho_atual}"
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, headers=headers)
            
            if response.status_code == 404:
                # Criar pasta
                parent_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root"
                if caminho_anterior:
                    parent_url += f":/{caminho_anterior}"
                parent_url += ":/children"
                
                create_body = {
                    "name": parte,
                    "folder": {},
                    "@microsoft.graph.conflictBehavior": "rename"
                }
                
                create_response = requests.post(
                    parent_url, 
                    headers={**headers, "Content-Type": "application/json"}, 
                    json=create_body
                )
                
                if create_response.status_code not in [200, 201]:
                    logger.warning(f"Não foi possível criar pasta {parte}")
                    
    except Exception as e:
        logger.warning(f"Erro ao criar estrutura de pastas: {e}")

# === UPLOAD E BACKUP ===
def mover_arquivo_existente(nome_arquivo, token):
    """Move arquivo existente para backup antes de substituir"""
    try:
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA}/{nome_arquivo}"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            file_id = response.json().get("id")
            timestamp = datetime.now().strftime("%Y-%m-%d_%Hh%M")
            nome_base = nome_arquivo.replace(".xlsx", "")
            novo_nome = f"{nome_base}_backup_{timestamp}.xlsx"
            
            patch_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/items/{file_id}"
            patch_body = {"name": novo_nome}
            patch_headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            patch_response = requests.patch(patch_url, headers=patch_headers, json=patch_body)
            
            if patch_response.status_code in [200, 201]:
                st.info(f"💾 Backup criado: {novo_nome}")
            else:
                st.warning(f"⚠️ Não foi possível criar backup do arquivo existente")
                
    except Exception as e:
        st.warning(f"⚠️ Erro ao processar backup: {str(e)}")
        logger.error(f"Erro no backup: {e}")

def upload_onedrive(nome_arquivo, conteudo_arquivo, token):
    """Faz upload de arquivo para OneDrive"""
    try:
        # Garantir que a pasta existe
        pasta_arquivo = "/".join(nome_arquivo.split("/")[:-1]) if "/" in nome_arquivo else ""
        if pasta_arquivo:
            criar_pasta_se_nao_existir(f"{PASTA}/{pasta_arquivo}", token)
        
        # Fazer backup se arquivo já existir
        if "/" not in nome_arquivo:  # Arquivo na raiz
            mover_arquivo_existente(nome_arquivo, token)
        
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA}/{nome_arquivo}:/content"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream"
        }
        response = requests.put(url, headers=headers, data=conteudo_arquivo)
        
        return response.status_code in [200, 201], response.status_code, response.text
        
    except Exception as e:
        logger.error(f"Erro no upload: {e}")
        return False, 500, f"Erro interno: {str(e)}"

# === FUNÇÕES DE CONSOLIDAÇÃO ===
def validar_dados_enviados(df, responsavel):
    """Valida os dados enviados pelo usuário"""
    erros = []
    avisos = []
    linhas_invalidas_detalhes = []
    
    # Validar responsável
    if not responsavel or not responsavel.strip():
        erros.append("⚠️ O nome do responsável é obrigatório")
    elif len(responsavel.strip()) < 2:
        erros.append("⚠️ O nome do responsável deve ter pelo menos 2 caracteres")
    
    # Validar se DataFrame não está vazio
    if df.empty:
        erros.append("❌ A planilha está vazia")
        return erros, avisos, linhas_invalidas_detalhes
    
    # Validar se existe coluna DATA
    if "DATA" not in df.columns:
        erros.append("⚠️ A planilha deve conter uma coluna 'DATA'")
        avisos.append("📋 Lembre-se: o arquivo deve ter uma aba chamada 'Vendas CTs' com a coluna 'DATA'")
    else:
        # Validar se as datas são válidas
        df_temp = df.copy()
        df_temp["DATA_CONVERTIDA"] = pd.to_datetime(df_temp["DATA"], errors="coerce")
        
        # Identificar linhas com datas inválidas
        linhas_invalidas_mask = df_temp["DATA_CONVERTIDA"].isna()
        linhas_invalidas = df_temp[linhas_invalidas_mask]
        datas_validas = df_temp["DATA_CONVERTIDA"].notna().sum()
        
        if datas_validas == 0:
            erros.append("❌ Nenhuma data válida encontrada na coluna 'DATA'")
        elif len(linhas_invalidas) > 0:
            # Preparar detalhes das linhas inválidas para exibição posterior
            for idx, row in linhas_invalidas.iterrows():
                linha_excel = idx + 2  # +2 porque Excel começa em 1 e tem cabeçalho
                valor_data = str(row["DATA"]) if pd.notna(row["DATA"]) else "VAZIO"
                linhas_invalidas_detalhes.append({
                    "Linha no Excel": linha_excel,
                    "Data Inválida": valor_data
                })
            
            avisos.append(f"⚠️ {len(linhas_invalidas)} linhas com datas inválidas serão ignoradas na consolidação")
    
    # Validar duplicatas na planilha enviada
    if not df.empty and "DATA" in df.columns:
        df_temp = df.copy()
        df_temp["DATA"] = pd.to_datetime(df_temp["DATA"], errors="coerce")
        df_temp = df_temp.dropna(subset=["DATA"])
        
        if not df_temp.empty:
            duplicatas = df_temp.duplicated(subset=["DATA"], keep=False).sum()
            if duplicatas > 0:
                avisos.append(f"⚠️ {duplicatas} linhas com datas duplicadas na planilha")
    
    return erros, avisos, linhas_invalidas_detalhes

def processar_consolidacao(df_novo, responsavel, token):
    """Processa a consolidação dos dados - Atualiza ou insere linha por linha"""
    consolidado_nome = "Reports_Geral_Consolidado.xlsx"

    # 1. Baixar arquivo consolidado existente
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA}/{consolidado_nome}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    
    with st.spinner("📥 Baixando arquivo consolidado existente..."):
        r = requests.get(url, headers=headers)

    if r.status_code == 200:
        try:
            df_consolidado = pd.read_excel(BytesIO(r.content))
            df_consolidado.columns = df_consolidado.columns.str.strip().str.upper()
            st.info(f"📂 Arquivo consolidado existente carregado ({len(df_consolidado)} registros)")
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
    
    # Converter datas e remover linhas inválidas
    df_novo["DATA"] = pd.to_datetime(df_novo["DATA"], errors="coerce")
    linhas_invalidas = df_novo["DATA"].isna().sum()
    df_novo = df_novo.dropna(subset=["DATA"])

    if df_novo.empty:
        st.error("❌ Nenhum registro válido para consolidar")
        return False

    if linhas_invalidas > 0:
        st.info(f"🧹 {linhas_invalidas} linhas com datas inválidas foram removidas")

    # 3. Consolidar linha por linha (comparação completa)
    registros_atualizados = 0
    registros_inseridos = 0
    registros_ignorados = 0
    
    with st.spinner("🔄 Processando consolidação..."):
        if not df_consolidado.empty:
            df_consolidado["DATA"] = pd.to_datetime(df_consolidado["DATA"], errors="coerce")
            df_consolidado = df_consolidado.dropna(subset=["DATA"])
            colunas = df_novo.columns.tolist()

            # Garantir que as colunas existem no consolidado
            for col in colunas:
                if col not in df_consolidado.columns:
                    df_consolidado[col] = None

            for idx, row_nova in df_novo.iterrows():
                # Buscar registros com mesma data e responsável
                cond = (
                    (df_consolidado["DATA"].dt.normalize() == row_nova["DATA"].normalize()) &
                    (df_consolidado["RESPONSÁVEL"].str.strip().str.upper() == row_nova["RESPONSÁVEL"].strip().upper())
                )
                possiveis = df_consolidado[cond]

                # Verificar se já existe linha idêntica
                if not possiveis.empty:
                    # Comparar valores das colunas principais (exceto metadados)
                    colunas_comparacao = [col for col in colunas if col not in ["RESPONSÁVEL"]]
                    
                    for _, row_existente in possiveis.iterrows():
                        if all(pd.isna(row_nova[col]) and pd.isna(row_existente[col]) or 
                               str(row_nova[col]).strip() == str(row_existente[col]).strip() 
                               for col in colunas_comparacao if col in row_existente.index):
                            registros_ignorados += 1
                            break
                    else:
                        # Atualizar primeiro registro encontrado
                        index = possiveis.index[0]
                        df_consolidado.loc[index, colunas] = row_nova.values
                        registros_atualizados += 1
                else:
                    # Inserir novo registro
                    new_row = pd.DataFrame([row_nova])
                    df_consolidado = pd.concat([df_consolidado, new_row], ignore_index=True)
                    registros_inseridos += 1

            df_final = df_consolidado.copy()
        else:
            df_final = df_novo.copy()
            registros_inseridos = len(df_novo)
            st.info("📂 Primeiro envio - criando arquivo consolidado")

    # 4. Ordenar e salvar
    df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"], na_position='last').reset_index(drop=True)
    
    # Salvar em buffer
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df_final.to_excel(writer, index=False, sheet_name="Vendas CTs")
    buffer.seek(0)

    # 5. Salvar cópia do arquivo enviado
    salvar_arquivo_enviado(df_novo, responsavel, token)
    
    # 6. Upload do arquivo consolidado
    with st.spinner("📤 Enviando arquivo consolidado..."):
        sucesso, status, resposta = upload_onedrive(consolidado_nome, buffer.read(), token)

    if sucesso:
        st.success("✅ Consolidação realizada com sucesso!")
        
        # Métricas de resultado
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Total de registros", len(df_final))
        with col2:
            st.metric("➕ Inseridos", registros_inseridos)
        with col3:
            st.metric("🔁 Atualizados", registros_atualizados)
        with col4:
            st.metric("⏭️ Ignorados", registros_ignorados)
        
        # Informações do período
        if not df_novo.empty:
            data_min = df_novo["DATA"].min().strftime("%d/%m/%Y")
            data_max = df_novo["DATA"].max().strftime("%d/%m/%Y")
            st.info(f"📅 Período processado: {data_min} até {data_max}")
        
        return True
    else:
        st.error(f"❌ Erro no upload: Status {status}")
        if status != 500:  # Não mostrar erro interno completo
            st.code(resposta)
        return False

def salvar_arquivo_enviado(df, responsavel, token):
    """Salva uma cópia do arquivo enviado pelo responsável"""
    try:
        if not df.empty and "DATA" in df.columns:
            data_base = df["DATA"].min()
            nome_pasta = f"Relatorios_Enviados/{data_base.strftime('%Y-%m')}"
            timestamp = datetime.now().strftime('%d-%m-%Y_%Hh%M')
            
            # Limpar nome do responsável para uso em arquivo
            responsavel_limpo = "".join(c for c in responsavel.strip() if c.isalnum() or c in (' ', '-', '_')).strip()
            nome_arquivo = f"{nome_pasta}/{responsavel_limpo}_{timestamp}.xlsx"
            
            # Salvar arquivo
            buffer_envio = BytesIO()
            with pd.ExcelWriter(buffer_envio, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name="Vendas CTs")
            buffer_envio.seek(0)
            
            sucesso, _, _ = upload_onedrive(nome_arquivo, buffer_envio.read(), token)
            if sucesso:
                st.info(f"💾 Cópia salva em: {nome_arquivo}")
            else:
                st.warning("⚠️ Não foi possível salvar cópia do arquivo enviado")
                
    except Exception as e:
        st.warning(f"⚠️ Não foi possível salvar cópia do arquivo: {e}")
        logger.error(f"Erro ao salvar arquivo enviado: {e}")

# === INTERFACE STREAMLIT ===
def main():
    st.set_page_config(
        page_title="Upload e Gestão de Planilhas", 
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Header com logo (se disponível)
    st.markdown(
        '''
        <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 20px;">
            <h2 style="margin: 0; color: #2E8B57;">📊 DSView BI – Upload de Planilhas</h2>
        </div>
        ''',
        unsafe_allow_html=True
    )

    # Sidebar navigation
    st.sidebar.markdown("### 📤 Upload de Planilhas")
    st.sidebar.markdown("Sistema de consolidação de relatórios")
    st.sidebar.divider()
    st.sidebar.markdown("**Status do Sistema:**")
    
    # Verificar autenticação
    token = obter_token()
    if not token:
        st.sidebar.error("❌ Desconectado")
        st.error("❌ Não foi possível autenticar. Verifique as credenciais.")
        st.stop()
    else:
        st.sidebar.success("✅ Conectado")

    st.markdown("## 📤 Upload de Planilha Excel")
    st.divider()

    # Upload de arquivo
    uploaded_file = st.file_uploader(
        "Escolha um arquivo Excel", 
        type=["xlsx", "xls"],
        help="Formatos aceitos: .xlsx, .xls"
    )

    # Campo obrigatório para responsável
    responsavel = st.text_input(
        "Digite seu nome (responsável): *", 
        placeholder="Ex: João Silva",
        help="Este campo é obrigatório",
        max_chars=100
    )

    # Processar arquivo carregado
    df = None
    if uploaded_file:
        try:
            # Detectar tipo de arquivo
            file_extension = uploaded_file.name.split('.')[-1].lower()
            
            with st.spinner("📖 Lendo arquivo..."):
                if file_extension == 'xls':
                    xls = pd.ExcelFile(uploaded_file, engine='xlrd')
                else:
                    xls = pd.ExcelFile(uploaded_file)
                
                sheets = xls.sheet_names
                
                # Selecionar aba
                if len(sheets) > 1:
                    # Verificar se existe aba "Vendas CTs" 
                    if "Vendas CTs" in sheets:
                        sheet = "Vendas CTs"
                        st.info("✅ Aba 'Vendas CTs' encontrada e selecionada automaticamente")
                    else:
                        sheet = st.selectbox(
                            "Selecione a aba (recomendado: 'Vendas CTs'):", 
                            sheets,
                            help="Para melhor compatibilidade, use uma aba chamada 'Vendas CTs'"
                        )
                        if sheet != "Vendas CTs":
                            st.warning("⚠️ Recomendamos que a aba seja chamada 'Vendas CTs'")
                else:
                    sheet = sheets[0]
                    if sheet != "Vendas CTs":
                        st.warning(f"⚠️ A aba atual se chama '{sheet}'. Recomendamos renomeá-la para 'Vendas CTs'")
                
                # Ler dados
                df = pd.read_excel(uploaded_file, sheet_name=sheet)
                df.columns = df.columns.str.strip().str.upper()
                
            st.success(f"✅ Arquivo carregado: {uploaded_file.name}")
            
        except Exception as e:
            st.error(f"❌ Erro ao ler o Excel: {e}")
            logger.error(f"Erro ao ler Excel: {e}")

    # Mostrar prévia e validações
    if df is not None:
        # Prévia dos dados
        st.subheader("👀 Prévia dos dados")
        st.dataframe(df.head(10), use_container_width=True, height=300)

        # Resumo dos dados
        st.subheader("📊 Resumo dos dados")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Linhas", df.shape[0])
        with col2:
            st.metric("Colunas", df.shape[1])
        with col3:
            if "DATA" in df.columns:
                datas_validas = pd.to_datetime(df["DATA"], errors="coerce").notna().sum()
                st.metric("Datas válidas", datas_validas)

        # Verificar colunas com valores nulos
        colunas_nulas = df.columns[df.isnull().any()].tolist()
        if colunas_nulas:
            st.warning(f"⚠️ Colunas com valores nulos: {', '.join(colunas_nulas[:5])}")
            if len(colunas_nulas) > 5:
                st.warning(f"... e mais {len(colunas_nulas) - 5} colunas")
        else:
            st.success("✅ Nenhuma coluna com valores nulos.")

        # Validações antes do envio
        st.subheader("🔍 Validações")
        erros, avisos, linhas_invalidas_detalhes = validar_dados_enviados(df, responsavel)
        
        # Mostrar avisos
        for aviso in avisos:
            st.warning(aviso)
        
        # Mostrar detalhes das linhas inválidas se existirem
        if linhas_invalidas_detalhes:
            st.error("❗ **ATENÇÃO: As seguintes linhas têm datas inválidas e NÃO serão incluídas na consolidação:**")
            
            # Converter para DataFrame para melhor visualização
            df_invalidas = pd.DataFrame(linhas_invalidas_detalhes)
            
            # Limitar exibição para não sobrecarregar
            if len(df_invalidas) <= 20:
                st.dataframe(df_invalidas, use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_invalidas.head(20), use_container_width=True, hide_index=True)
                st.warning(f"... e mais {len(df_invalidas) - 20} linhas com datas inválidas")
            
            st.error("🔧 **Para incluir essas linhas:** Corrija as datas na planilha original e envie novamente.")
        
        # Mostrar erros
        if erros:
            for erro in erros:
                st.error(erro)
        else:
            if not linhas_invalidas_detalhes:
                st.success("✅ Todos os dados estão válidos para consolidação")
            else:
                st.warning("⚠️ Dados válidos serão consolidados. Corrija as datas inválidas para incluir todas as linhas.")

        # Botão de envio
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("📧 Consolidar Dados", type="primary", disabled=bool(erros)):
                if erros:
                    st.error("❌ Corrija os erros acima antes de prosseguir")
                else:
                    sucesso = processar_consolidacao(df, responsavel, token)
                    if sucesso:
                        st.balloons()
                        
        with col2:
            if st.button("🔄 Limpar", type="secondary"):
                st.rerun()

    # Rodapé com informações
    st.divider()
    st.markdown(
        """
        <div style="text-align: center; color: #666; font-size: 0.8em;">
            DSView BI - Sistema de Consolidação de Relatórios<br>
            ⚠️ Certifique-se de que sua planilha contenha:<br>
            • Uma aba chamada <strong>'Vendas CTs'</strong><br>
            • Uma coluna <strong>'DATA'</strong><br>
            • Informe o nome do <strong>responsável</strong>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()