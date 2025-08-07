import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
from msal import ConfidentialClientApplication
import unicodedata
import logging
import re

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
def extrair_responsavel_da_planilha(df):
    """Extrai o nome do responsável da planilha usando diferentes estratégias"""
    responsavel_encontrado = None
    metodo_deteccao = None
    
    try:
        # Estratégia 1: Procurar coluna 'RESPONSÁVEL' ou 'RESPONSAVEL'
        colunas_responsavel = [col for col in df.columns if 
                             any(term in col.upper() for term in ['RESPONSÁVEL', 'RESPONSAVEL', 'RESP'])]
        
        if colunas_responsavel:
            coluna = colunas_responsavel[0]
            valores_unicos = df[coluna].dropna().unique()
            if len(valores_unicos) > 0:
                # Pegar o valor mais comum
                responsavel_encontrado = df[coluna].value_counts().index[0]
                metodo_deteccao = f"Coluna '{coluna}'"
        
        # Estratégia 2: Procurar em células específicas (primeira linha, primeiras colunas)
        if not responsavel_encontrado:
            # Verificar primeira linha em busca de padrões como "Responsável:", "Nome:", etc.
            primeira_linha = df.iloc[0] if not df.empty else pd.Series()
            for col in df.columns[:5]:  # Verificar apenas primeiras 5 colunas
                valor = str(primeira_linha.get(col, '')).strip()
                if valor and len(valor) > 2 and any(char.isalpha() for char in valor):
                    # Verificar se parece com um nome (tem espaço e letras)
                    if ' ' in valor and len(valor.split()) >= 2:
                        responsavel_encontrado = valor
                        metodo_deteccao = f"Primeira linha, coluna '{col}'"
                        break
        
        # Estratégia 3: Procurar em células que contenham padrões de nome
        if not responsavel_encontrado:
            for idx, row in df.head(10).iterrows():  # Verificar apenas primeiras 10 linhas
                for col in df.columns:
                    valor = str(row[col]).strip()
                    if (valor and len(valor) > 2 and 
                        ' ' in valor and 
                        len(valor.split()) >= 2 and
                        not valor.replace(' ', '').isdigit() and  # Não é apenas números
                        not '/' in valor and  # Não é data
                        not valor.upper() in ['NAN', 'NULL', 'NONE']):
                        
                        # Verificar se parece com nome (mais de 50% letras)
                        letras = sum(c.isalpha() for c in valor)
                        if letras / len(valor) > 0.5:
                            responsavel_encontrado = valor
                            metodo_deteccao = f"Linha {idx+1}, coluna '{col}'"
                            break
                if responsavel_encontrado:
                    break
        
        # Estratégia 4: Procurar em metadados do Excel (se disponível)
        # Esta seria implementada com openpyxl se necessário
        
        # Limpar e validar o nome encontrado
        if responsavel_encontrado:
            # Remover caracteres especiais no início/fim
            responsavel_encontrado = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', responsavel_encontrado).strip()
            
            # Capitalizar adequadamente
            responsavel_encontrado = ' '.join(word.capitalize() for word in responsavel_encontrado.split())
            
            # Verificar se o nome é válido (pelo menos 2 palavras, cada uma com pelo menos 2 caracteres)
            palavras = responsavel_encontrado.split()
            if len(palavras) < 2 or any(len(palavra) < 2 for palavra in palavras):
                responsavel_encontrado = None
                metodo_deteccao = None
    
    except Exception as e:
        logger.error(f"Erro ao extrair responsável: {e}")
        responsavel_encontrado = None
        metodo_deteccao = None
    
    return responsavel_encontrado, metodo_deteccao

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
    elif len(responsavel.strip().split()) < 2:
        avisos.append("⚠️ Recomenda-se informar nome e sobrenome do responsável")
    
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
    
    # Validar colunas essenciais
    colunas_essenciais = ['TMO - DUTO', 'TMO - FREIO', 'TMO - SANIT', 'TMO - VERNIZ', 'CX EVAP']
    colunas_faltantes = [col for col in colunas_essenciais if col not in df.columns]
    
    if colunas_faltantes:
        avisos.append(f"⚠️ Colunas recomendadas não encontradas: {', '.join(colunas_faltantes)}")
    
    return erros, avisos, linhas_invalidas_detalhes

def processar_consolidacao(df_novo, responsavel, token):
    """Processa a consolidação dos dados - Comparação linha por linha por RESPONSÁVEL + DATA"""
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

    # 3. Consolidação linha por linha
    registros_inseridos = 0
    registros_atualizados = 0
    registros_sem_alteracao = 0
    detalhes_operacoes = []
    
    with st.spinner("🔄 Processando consolidação linha por linha..."):
        if not df_consolidado.empty:
            # Converter datas do consolidado
            df_consolidado["DATA"] = pd.to_datetime(df_consolidado["DATA"], errors="coerce")
            df_consolidado = df_consolidado.dropna(subset=["DATA"])
            
            # Garantir que todas as colunas do novo estão no consolidado
            for col in df_novo.columns:
                if col not in df_consolidado.columns:
                    df_consolidado[col] = None
                    
            # Garantir que todas as colunas do consolidado estão no novo
            for col in df_consolidado.columns:
                if col not in df_novo.columns:
                    df_novo[col] = None
            
            # Processar cada linha do arquivo novo
            for idx, linha_nova in df_novo.iterrows():
                data_nova = linha_nova["DATA"]
                responsavel_novo = linha_nova["RESPONSÁVEL"]
                
                # Buscar linha correspondente no consolidado (RESPONSÁVEL + DATA)
                mask_correspondencia = (
                    (df_consolidado["DATA"].dt.normalize() == data_nova.normalize()) &
                    (df_consolidado["RESPONSÁVEL"].str.strip().str.upper() == responsavel_novo.strip().upper())
                )
                
                linhas_correspondentes = df_consolidado[mask_correspondencia]
                
                if linhas_correspondentes.empty:
                    # Registro não existe - INSERIR
                    nova_linha = pd.DataFrame([linha_nova])
                    df_consolidado = pd.concat([df_consolidado, nova_linha], ignore_index=True)
                    registros_inseridos += 1
                    detalhes_operacoes.append({
                        "Operação": "INSERIR",
                        "Data": data_nova.strftime("%d/%m/%Y"),
                        "Responsável": responsavel_novo
                    })
                    
                else:
                    # Registro existe - VERIFICAR SE HOUVE ALTERAÇÃO
                    linha_existente = linhas_correspondentes.iloc[0]
                    index_existente = linhas_correspondentes.index[0]
                    
                    # Comparar todas as colunas (exceto metadados)
                    colunas_comparacao = [col for col in df_novo.columns 
                                        if col not in ["ÚLTIMA_ATUALIZAÇÃO"]]
                    
                    houve_alteracao = False
                    campos_alterados = []
                    
                    for col in colunas_comparacao:
                        valor_novo = linha_nova[col]
                        valor_existente = linha_existente[col]
                        
                        # Normalizar valores para comparação
                        if pd.isna(valor_novo) and pd.isna(valor_existente):
                            continue  # Ambos são NaN - sem alteração
                        elif pd.isna(valor_novo) or pd.isna(valor_existente):
                            houve_alteracao = True
                            campos_alterados.append(col)
                        else:
                            # Converter para string e comparar (para evitar problemas de tipo)
                            if str(valor_novo).strip() != str(valor_existente).strip():
                                houve_alteracao = True
                                campos_alterados.append(col)
                    
                    if houve_alteracao:
                        # ATUALIZAR linha existente
                        for col in colunas_comparacao:
                            df_consolidado.loc[index_existente, col] = linha_nova[col]
                        
                        registros_atualizados += 1
                        detalhes_operacoes.append({
                            "Operação": "ATUALIZAR",
                            "Data": data_nova.strftime("%d/%m/%Y"),
                            "Responsável": responsavel_novo,
                            "Campos alterados": ", ".join(campos_alterados[:3]) + ("..." if len(campos_alterados) > 3 else "")
                        })
                    else:
                        # Sem alteração
                        registros_sem_alteracao += 1
                        detalhes_operacoes.append({
                            "Operação": "SEM ALTERAÇÃO",
                            "Data": data_nova.strftime("%d/%m/%Y"),
                            "Responsável": responsavel_novo
                        })
                        
            df_final = df_consolidado.copy()
            
        else:
            # Arquivo consolidado não existe - inserir todos os registros
            df_final = df_novo.copy()
            registros_inseridos = len(df_novo)
            st.info("📂 Primeiro envio - criando arquivo consolidado")

    # 4. Ordenar e finalizar
    df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"], na_position='last').reset_index(drop=True)
    
    # Adicionar metadados de controle
    df_final["ÚLTIMA_ATUALIZAÇÃO"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
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
            st.metric("🔄 Atualizados", registros_atualizados)
        with col4:
            st.metric("➡️ Sem alteração", registros_sem_alteracao)
        
        # Mostrar detalhes das operações
        if detalhes_operacoes:
            with st.expander("📋 Detalhes das Operações Realizadas"):
                df_detalhes = pd.DataFrame(detalhes_operacoes)
                st.dataframe(df_detalhes, use_container_width=True, hide_index=True)
        
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

    # Processar arquivo carregado
    df = None
    responsavel_auto = None
    metodo_deteccao = None
    
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
                
                # Tentar extrair responsável automaticamente
                responsavel_auto, metodo_deteccao = extrair_responsavel_da_planilha(df)
                
            st.success(f"✅ Arquivo carregado: {uploaded_file.name}")
            
            # Mostrar informação sobre detecção automática do responsável
            if responsavel_auto:
                st.success(f"🎯 **Responsável detectado automaticamente:** {responsavel_auto}")
                st.info(f"📍 **Método de detecção:** {metodo_deteccao}")
            else:
                st.warning("⚠️ Não foi possível detectar automaticamente o responsável. Digite manualmente abaixo.")
            
        except Exception as e:
            st.error(f"❌ Erro ao ler o Excel: {e}")
            logger.error(f"Erro ao ler Excel: {e}")

    # Campo para responsável (preenchido automaticamente se detectado)
    if 'responsavel_auto' not in st.session_state:
        st.session_state.responsavel_auto = ""
    
    # Atualizar o valor automático se foi detectado
    if responsavel_auto and st.session_state.responsavel_auto != responsavel_auto:
        st.session_state.responsavel_auto = responsavel_auto

    # Campo obrigatório para responsável
    responsavel = st.text_input(
        "Digite seu nome (responsável): *", 
        value=st.session_state.responsavel_auto,
        placeholder="Ex: João Silva",
        help="Este campo é obrigatório. O sistema tentará detectar automaticamente da planilha.",
        max_chars=100
    )

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

        # Resumo de totais por produto
        st.subheader("💰 Resumo de Totais por Produto")
        
        # Lista das colunas de produtos corrigidas
        colunas_produtos = ['TMO - DUTO', 'TMO - FREIO', 'TMO - SANIT', 'TMO - VERNIZ', 'CX EVAP']
        
        # Encontrar colunas que existem no DataFrame
        colunas_encontradas = [col for col in colunas_produtos if col in df.columns]
        
        if colunas_encontradas:
            # Calcular totais
            totais = {}
            total_geral = 0
            
            for coluna in colunas_encontradas:
                # Converter para numérico, tratando erros como 0
                valores_numericos = pd.to_numeric(df[coluna], errors='coerce').fillna(0)
                total = int(valores_numericos.sum())  # Converter para inteiro
                totais[coluna] = total
                total_geral += total
            
            # Calcular TMO - Total se houver colunas TMO
            colunas_tmo = [col for col in colunas_encontradas if col.startswith('TMO -')]
            if colunas_tmo:
                tmo_total = sum(totais[col] for col in colunas_tmo)
                totais['TMO - TOTAL'] = tmo_total
            
            # Exibir métricas em colunas
            produtos_para_exibir = [col for col in colunas_produtos if col in totais]
            if 'TMO - TOTAL' in totais:
                produtos_para_exibir.append('TMO - TOTAL')
            
            num_colunas = len(produtos_para_exibir)
            cols = st.columns(num_colunas)
            
            # Mostrar totais por produto
            for i, coluna in enumerate(produtos_para_exibir):
                with cols[i]:
                    total = totais[coluna]
                    # Formatar número com separadores de milhares (formato inteiro)
                    total_formatado = f"{total:,}".replace(',', '.')
                    
                    # Definir emoji baseado no produto
                    if 'DUTO' in coluna:
                        emoji = "🔧"
                    elif 'FREIO' in coluna:
                        emoji = "🚗"
                    elif 'SANIT' in coluna:
                        emoji = "🧽"
                    elif 'VERNIZ' in coluna:
                        emoji = "🎨"
                    elif 'EVAP' in coluna:
                        emoji = "📦"
                    elif 'TOTAL' in coluna:
                        emoji = "💰"
                    else:
                        emoji = "📊"
                    
                    # Nome simplificado para exibição
                    nome_display = coluna.replace('TMO - ', '').title()
                    
                    st.metric(f"{emoji} {nome_display}", total_formatado)
            
            # Tabela resumo adicional
            with st.expander("📋 Detalhes dos Totais"):
                resumo_data = []
                for coluna in produtos_para_exibir:
                    total = totais[coluna]
                    nome_produto = coluna.replace('TMO - ', '').title()
                    resumo_data.append({
                        'Produto': nome_produto,
                        'Total': f"{total:,}".replace(',', '.'),
                        'Registros': (pd.to_numeric(df[coluna], errors='coerce') > 0).sum() if coluna in df.columns else 0
                    })
                
                df_resumo = pd.DataFrame(resumo_data)
                st.dataframe(df_resumo, use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ Nenhuma coluna de produtos encontrada")
            
            # Mostrar colunas disponíveis para ajudar o usuário
            with st.expander("🔍 Ver colunas disponíveis"):
                colunas_disponiveis = [col for col in df.columns if col != 'DATA']
                st.write("**Colunas encontradas na planilha:**")
                for col in colunas_disponiveis:
                    st.write(f"• {col}")
                st.info("💡 **Dica:** Renomeie as colunas na sua planilha para: TMO - Duto, TMO - Freio, TMO - Sanit, TMO - Verniz, CX EVAP")

        # Análise de qualidade dos dados
        st.subheader("🔍 Análise de Qualidade dos Dados")
        col1, col2 = st.columns(2)
        
        with col1:
            # Verificar colunas com valores nulos
            colunas_nulas = df.columns[df.isnull().any()].tolist()
            if colunas_nulas:
                st.warning(f"⚠️ Colunas com valores nulos: {len(colunas_nulas)}")
                with st.expander("Ver detalhes"):
                    for col in colunas_nulas[:10]:  # Mostrar apenas as primeiras 10
                        nulos = df[col].isnull().sum()
                        percentual = (nulos / len(df)) * 100
                        st.write(f"• **{col}**: {nulos} nulos ({percentual:.1f}%)")
                    if len(colunas_nulas) > 10:
                        st.write(f"... e mais {len(colunas_nulas) - 10} colunas")
            else:
                st.success("✅ Nenhuma coluna com valores nulos")
        
        with col2:
            # Verificar período dos dados
            if "DATA" in df.columns:
                datas_validas = pd.to_datetime(df["DATA"], errors="coerce")
                datas_validas = datas_validas.dropna()
                
                if not datas_validas.empty:
                    data_min = datas_validas.min()
                    data_max = datas_validas.max()
                    dias_periodo = (data_max - data_min).days + 1
                    
                    st.info(f"📅 **Período:** {data_min.strftime('%d/%m/%Y')} a {data_max.strftime('%d/%m/%Y')}")
                    st.info(f"📊 **Duração:** {dias_periodo} dias")
                    
                    # Verificar se há dados muito antigos ou muito futuros
                    hoje = datetime.now()
                    if data_max > hoje:
                        st.warning("⚠️ Há datas futuras na planilha")
                    if data_min < (hoje - pd.Timedelta(days=365)):
                        st.warning("⚠️ Há datas muito antigas (mais de 1 ano)")

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

        # Verificar se há conflitos potenciais
        if not df.empty and "DATA" in df.columns and responsavel:
            st.subheader("⚠️ Verificação de Conflitos")
            
            datas_validas = pd.to_datetime(df["DATA"], errors="coerce").dropna()
            if not datas_validas.empty:
                data_min = datas_validas.min()
                data_max = datas_validas.max()
                
                st.info(f"📋 **Resumo do envio:**")
                st.info(f"• **Responsável:** {responsavel}")
                st.info(f"• **Período:** {data_min.strftime('%d/%m/%Y')} até {data_max.strftime('%d/%m/%Y')}")
                st.info(f"• **Total de registros válidos:** {len(datas_validas)}")
                
                st.warning("⚠️ **IMPORTANTE:** Se você já enviou dados para este período, eles serão substituídos pelos novos dados.")

        # Botões de ação
        st.subheader("🚀 Ações")
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            if st.button("📧 Consolidar Dados", type="primary", disabled=bool(erros)):
                if erros:
                    st.error("❌ Corrija os erros acima antes de prosseguir")
                else:
                    with st.spinner("🔄 Processando consolidação..."):
                        sucesso = processar_consolidacao(df, responsavel, token)
                        if sucesso:
                            st.balloons()
                            st.success("🎉 Dados consolidados com sucesso!")
                            
                            # Limpar o cache do responsável automático
                            if 'responsavel_auto' in st.session_state:
                                del st.session_state.responsavel_auto
                        
        with col2:
            if st.button("🔄 Limpar Tudo", type="secondary"):
                # Limpar session state
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()
        
        with col3:
            if st.button("📊 Ver Consolidado", type="secondary"):
                st.info("🔧 Funcionalidade em desenvolvimento - Em breve você poderá visualizar o arquivo consolidado")

    # Instruções e dicas
    with st.expander("📚 Instruções e Dicas"):
        st.markdown("""
        ### 📋 **Checklist para envio:**
        
        ✅ **Estrutura da planilha:**
        - Aba chamada **'Vendas CTs'**
        - Coluna **'DATA'** com datas válidas
        - Colunas de produtos: **TMO - Duto, TMO - Freio, TMO - Sanit, TMO - Verniz, CX EVAP**
        
        ✅ **Responsável:**
        - O sistema tentará detectar automaticamente o responsável da planilha
        - Se não detectar, digite seu nome completo
        - Use sempre o mesmo nome para manter consistência
        
        ✅ **Datas:**
        - Use formato de data válido (dd/mm/aaaa ou mm/dd/aaaa)
        - Evite células vazias na coluna DATA
        - Verifique se não há datas futuras por engano
        
        ✅ **Dados:**
        - Valores numéricos nas colunas de produtos
        - Evite caracteres especiais desnecessários
        - Mantenha consistência nos nomes dos produtos
        
        ### 🔄 **Como funciona a consolidação:**
        
        1. **Detecção automática:** O sistema tenta encontrar o responsável na planilha
        2. **Validação:** Verifica estrutura, datas e dados
        3. **Substituição inteligente:** Remove dados antigos do mesmo responsável no mesmo período
        4. **Backup automático:** Cria backup do arquivo anterior
        5. **Consolidação:** Adiciona novos dados ao arquivo principal
        6. **Cópia de segurança:** Salva uma cópia do arquivo enviado
        
        ### 🆘 **Problemas comuns:**
        
        **❌ "Nenhuma data válida encontrada"**
        - Verifique se a coluna se chama exatamente 'DATA'
        - Confirme se as datas estão em formato válido
        
        **❌ "Responsável não detectado"**
        - Digite manualmente o nome do responsável
        - Na próxima versão, inclua uma coluna 'RESPONSÁVEL' na planilha
        
        **❌ "Colunas de produtos não encontradas"**
        - Renomeie as colunas conforme indicado acima
        - Mantenha a grafia exata, incluindo espaços e hífens
        """)

    # Rodapé com informações
    st.divider()
    st.markdown(
        """
        <div style="text-align: center; color: #666; font-size: 0.8em;">
            <strong>DSView BI - Sistema de Consolidação de Relatórios v2.0</strong><br>
            🔄 Detecção automática de responsável | 🛡️ Validações aprimoradas | 📊 Análise de qualidade<br>
            💾 Backup automático | 🔍 Verificação de conflitos | 📋 Logs detalhados
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()