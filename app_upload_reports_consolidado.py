import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
from msal import ConfidentialClientApplication
import unicodedata
import logging
import re
import os

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
def extrair_responsavel_do_arquivo(nome_arquivo):
    """Extrai o nome do responsável do nome do arquivo"""
    try:
        # Remove extensão
        nome_sem_extensao = os.path.splitext(nome_arquivo)[0]
        
        # Remove palavras comuns que não são nomes (opcional)
        palavras_ignorar = ['relatorio', 'report', 'vendas', 'dados', 'planilha', 'excel', 'cts']
        
        # Limpa o nome removendo caracteres especiais e números
        nome_limpo = re.sub(r'[0-9_-]', ' ', nome_sem_extensao)
        nome_limpo = re.sub(r'\s+', ' ', nome_limpo).strip()
        
        # Se o nome estiver vazio ou muito curto, usar nome do arquivo original
        if len(nome_limpo) < 3:
            nome_limpo = nome_sem_extensao.replace('_', ' ').replace('-', ' ')
        
        # Capitalizar primeira letra de cada palavra
        responsavel = ' '.join(word.capitalize() for word in nome_limpo.split() 
                              if word.lower() not in palavras_ignorar and len(word) > 1)
        
        # Se ainda estiver vazio, usar o nome original do arquivo
        if not responsavel.strip():
            responsavel = nome_sem_extensao
            
        return responsavel.strip()
        
    except Exception as e:
        logger.error(f"Erro ao extrair responsável: {e}")
        return nome_arquivo

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
def validar_dados_enviados(df, nome_arquivo):
    """Valida os dados enviados pelo usuário"""
    erros = []
    avisos = []
    linhas_invalidas_detalhes = []
    
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

def baixar_arquivo_consolidado(token):
    """Baixa o arquivo consolidado existente"""
    consolidado_nome = "Reports_Geral_Consolidado.xlsx"
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA}/{consolidado_nome}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            df_consolidado = pd.read_excel(BytesIO(response.content))
            df_consolidado.columns = df_consolidado.columns.str.strip().str.upper()
            return df_consolidado, True
        else:
            return pd.DataFrame(), False
            
    except Exception as e:
        logger.error(f"Erro ao baixar arquivo consolidado: {e}")
        return pd.DataFrame(), False

def comparar_e_atualizar_registros(df_consolidado, df_novo, responsavel):
    """Compara registros usando RESPONSÁVEL e DATA e atualiza conforme necessário"""
    registros_atualizados = 0
    registros_inseridos = 0
    registros_ignorados = 0
    
    if df_consolidado.empty:
        # Primeiro envio - todos os registros são novos
        df_final = df_novo.copy()
        registros_inseridos = len(df_novo)
        return df_final, registros_inseridos, registros_atualizados, registros_ignorados
    
    # Garantir que as colunas existem no consolidado
    colunas = df_novo.columns.tolist()
    for col in colunas:
        if col not in df_consolidado.columns:
            df_consolidado[col] = None
    
    # Processar cada linha do arquivo novo
    for idx, row_nova in df_novo.iterrows():
        data_nova = row_nova["DATA"]
        responsavel_novo = row_nova["RESPONSÁVEL"]
        
        # Buscar registros existentes com mesma data e responsável
        mask_existente = (
            (df_consolidado["DATA"].dt.normalize() == data_nova.normalize()) &
            (df_consolidado["RESPONSÁVEL"].str.strip().str.upper() == responsavel_novo.strip().upper())
        )
        
        registros_existentes = df_consolidado[mask_existente]
        
        if registros_existentes.empty:
            # Novo registro - inserir
            new_row = pd.DataFrame([row_nova])
            df_consolidado = pd.concat([df_consolidado, new_row], ignore_index=True)
            registros_inseridos += 1
        else:
            # Verificar se os valores são diferentes
            registro_existente = registros_existentes.iloc[0]
            
            # Comparar apenas colunas de dados (excluir metadados)
            colunas_comparacao = [col for col in colunas if col not in ["RESPONSÁVEL", "DATA"]]
            
            valores_diferentes = False
            for col in colunas_comparacao:
                if col in registro_existente.index:
                    valor_novo = row_nova[col]
                    valor_existente = registro_existente[col]
                    
                    # Tratar valores NaN e comparar
                    if pd.isna(valor_novo) and pd.isna(valor_existente):
                        continue
                    elif pd.isna(valor_novo) or pd.isna(valor_existente):
                        valores_diferentes = True
                        break
                    elif str(valor_novo).strip() != str(valor_existente).strip():
                        valores_diferentes = True
                        break
            
            if valores_diferentes:
                # Atualizar registro existente
                index_existente = registros_existentes.index[0]
                df_consolidado.loc[index_existente, colunas] = row_nova.values
                registros_atualizados += 1
            else:
                # Registro idêntico - ignorar
                registros_ignorados += 1
    
    return df_consolidado, registros_inseridos, registros_atualizados, registros_ignorados

def processar_consolidacao(df_novo, nome_arquivo, token):
    """Processa a consolidação dos dados com lógica melhorada"""
    
    # Extrair responsável do nome do arquivo
    responsavel = extrair_responsavel_do_arquivo(nome_arquivo)
    st.info(f"👤 Responsável identificado: **{responsavel}**")
    
    # 1. Baixar arquivo consolidado existente
    with st.spinner("📥 Baixando arquivo consolidado existente..."):
        df_consolidado, arquivo_existe = baixar_arquivo_consolidado(token)
    
    if arquivo_existe:
        st.info(f"📂 Arquivo consolidado carregado ({len(df_consolidado)} registros)")
    else:
        st.info("📂 Criando novo arquivo consolidado")

    # 2. Preparar dados novos
    df_novo = df_novo.copy()
    df_novo["RESPONSÁVEL"] = responsavel
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

    # 3. Processar consolidação com lógica melhorada
    with st.spinner("🔄 Processando consolidação..."):
        if not df_consolidado.empty:
            df_consolidado["DATA"] = pd.to_datetime(df_consolidado["DATA"], errors="coerce")
            df_consolidado = df_consolidado.dropna(subset=["DATA"])
        
        df_final, inseridos, atualizados, ignorados = comparar_e_atualizar_registros(
            df_consolidado, df_novo, responsavel
        )

    # 4. Ordenar por data e responsável
    df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"], na_position='last').reset_index(drop=True)
    
    # 5. Salvar arquivo enviado com nome original
    salvar_arquivo_enviado(df_novo, nome_arquivo, responsavel, token)
    
    # 6. Salvar arquivo consolidado
    with st.spinner("📤 Salvando arquivo consolidado..."):
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False, sheet_name="Vendas CTs")
        buffer.seek(0)
        
        consolidado_nome = "Reports_Geral_Consolidado.xlsx"
        sucesso, status, resposta = upload_onedrive(consolidado_nome, buffer.read(), token)

    if sucesso:
        st.success("✅ Consolidação realizada com sucesso!")
        
        # Métricas de resultado
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Total de registros", len(df_final))
        with col2:
            st.metric("➕ Inseridos", inseridos)
        with col3:
            st.metric("🔁 Atualizados", atualizados)
        with col4:
            st.metric("⏭️ Ignorados", ignorados)
        
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

def salvar_arquivo_enviado(df, nome_arquivo_original, responsavel, token):
    """Salva o arquivo enviado com o nome original na pasta de enviados"""
    try:
        if not df.empty and "DATA" in df.columns:
            data_base = df["DATA"].min()
            nome_pasta = f"Relatorios_Enviados/{data_base.strftime('%Y-%m')}"
            timestamp = datetime.now().strftime('%d-%m-%Y_%Hh%M')
            
            # Usar nome original do arquivo com timestamp
            nome_sem_extensao = os.path.splitext(nome_arquivo_original)[0]
            nome_arquivo = f"{nome_pasta}/{nome_sem_extensao}_{timestamp}.xlsx"
            
            # Salvar arquivo
            buffer_envio = BytesIO()
            with pd.ExcelWriter(buffer_envio, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name="Vendas CTs")
            buffer_envio.seek(0)
            
            sucesso, _, _ = upload_onedrive(nome_arquivo, buffer_envio.read(), token)
            if sucesso:
                st.info(f"💾 Arquivo salvo como: {nome_arquivo}")
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
    st.info("💡 **Novidade**: O responsável será identificado automaticamente pelo nome do arquivo!")
    st.divider()

    # Upload de arquivo
    uploaded_file = st.file_uploader(
        "Escolha um arquivo Excel", 
        type=["xlsx", "xls"],
        help="Formatos aceitos: .xlsx, .xls | O responsável será extraído do nome do arquivo"
    )

    # Processar arquivo carregado
    df = None
    if uploaded_file:
        try:
            # Mostrar responsável identificado
            responsavel_identificado = extrair_responsavel_do_arquivo(uploaded_file.name)
            st.success(f"📁 Arquivo: **{uploaded_file.name}**")
            st.info(f"👤 Responsável identificado: **{responsavel_identificado}**")
            
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
                        st.success("✅ Aba 'Vendas CTs' encontrada e selecionada automaticamente")
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
                
            st.success(f"✅ Dados carregados com sucesso!")
            
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
        erros, avisos, linhas_invalidas_detalhes = validar_dados_enviados(df, uploaded_file.name)
        
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
                    sucesso = processar_consolidacao(df, uploaded_file.name, token)
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
            • Colunas: <strong>TMO - Duto, TMO - Freio, TMO - Sanit, TMO - Verniz, CX EVAP</strong><br>
            • O responsável será <strong>identificado automaticamente</strong> pelo nome do arquivo
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()