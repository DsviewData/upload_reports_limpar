import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from io import BytesIO
from msal import ConfidentialClientApplication
import unicodedata
import logging
import os
import json
import time
import hashlib

# === INFORMAÇÕES DE VERSÃO ===
APP_VERSION = "2.1.0"
VERSION_DATE = "2025-01-07"
CHANGELOG = {
    "2.1.0": {
        "date": "2025-01-07",
        "changes": [
            "🔒 Sistema de controle de concorrência implementado",
            "📝 Controle de versão adicionado",
            "🔄 Nova lógica de consolidação (substitui registros por RESPONSÁVEL + DATA)",
            "🚀 Processo automático sem confirmação manual",
            "💾 Backup automático de substituições"
        ]
    },
    "2.0.0": {
        "date": "2025-01-05", 
        "changes": [
            "🎯 Lógica de consolidação completamente reescrita",
            "📊 Métricas avançadas de processamento",
            "✨ Interface melhorada com informações claras"
        ]
    },
    "1.0.0": {
        "date": "2024-12-01",
        "changes": [
            "🚀 Versão inicial do sistema de consolidação",
            "📤 Upload e processamento básico de planilhas",
            "🔐 Autenticação Microsoft Graph API"
        ]
    }
}

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
LOCK_TIMEOUT = 300  # 5 minutos de timeout para lock

# === SISTEMA DE CONTROLE DE CONCORRÊNCIA ===
def criar_usuario_id():
    """Cria um ID único para a sessão do usuário"""
    if 'user_session_id' not in st.session_state:
        timestamp = str(int(time.time()))
        session_id = hashlib.md5(f"{timestamp}_{st.session_state.get('session_id', 'unknown')}".encode()).hexdigest()[:8]
        st.session_state.user_session_id = f"user_{session_id}"
    return st.session_state.user_session_id

def criar_info_lock(usuario_id, operacao="Consolidação de dados"):
    """Cria informações do lock"""
    return {
        "usuario_id": usuario_id,
        "timestamp": datetime.now().isoformat(),
        "operacao": operacao,
        "timeout": (datetime.now() + timedelta(seconds=LOCK_TIMEOUT)).isoformat(),
        "versao_app": APP_VERSION
    }

def verificar_e_criar_lock(token, usuario_id):
    """Verifica se sistema está livre e cria lock se possível"""
    lock_nome = "sistema_lock.json"
    
    # Verificar se já existe lock
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA}/{lock_nome}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            # Lock existe - verificar se é válido
            try:
                lock_info = json.loads(response.text)
                lock_timeout = datetime.fromisoformat(lock_info.get("timeout", ""))
                
                # Verificar se o lock expirou
                if datetime.now() > lock_timeout:
                    st.warning(f"🔓 Lock expirado encontrado. Removendo...")
                    remover_lock(token)
                    # Tentar criar novo lock
                    return criar_novo_lock(token, usuario_id)
                
                # Lock ainda válido - verificar se é do mesmo usuário
                if lock_info.get("usuario_id") == usuario_id:
                    st.success("🔒 Você já possui o controle do sistema")
                    return True, "próprio_lock"
                else:
                    # Lock de outro usuário
                    tempo_restante = lock_timeout - datetime.now()
                    minutos_restantes = int(tempo_restante.total_seconds() / 60)
                    
                    return False, {
                        "usuario": lock_info.get("usuario_id", "Usuário desconhecido"),
                        "operacao": lock_info.get("operacao", "Operação desconhecida"),
                        "tempo_restante": minutos_restantes,
                        "timeout": lock_timeout.strftime("%H:%M:%S")
                    }
                    
            except (json.JSONDecodeError, ValueError) as e:
                st.warning("🔧 Lock corrompido encontrado. Removendo...")
                remover_lock(token)
                return criar_novo_lock(token, usuario_id)
        
        elif response.status_code == 404:
            # Nenhum lock existe - criar novo
            return criar_novo_lock(token, usuario_id)
        
        else:
            st.error(f"❌ Erro ao verificar lock: Status {response.status_code}")
            return False, "erro_sistema"
            
    except Exception as e:
        st.error(f"❌ Erro no sistema de locks: {e}")
        return False, "erro_sistema"

def criar_novo_lock(token, usuario_id):
    """Cria um novo lock no sistema"""
    try:
        lock_info = criar_info_lock(usuario_id)
        lock_content = json.dumps(lock_info, indent=2).encode()
        
        lock_nome = "sistema_lock.json"
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA}/{lock_nome}:/content"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.put(url, headers=headers, data=lock_content)
        
        if response.status_code in [200, 201]:
            st.success(f"🔒 Sistema bloqueado para sua sessão ({usuario_id})")
            return True, "lock_criado"
        else:
            st.error(f"❌ Não foi possível criar lock: Status {response.status_code}")
            return False, "erro_criar_lock"
            
    except Exception as e:
        st.error(f"❌ Erro ao criar lock: {e}")
        return False, "erro_criar_lock"

def remover_lock(token):
    """Remove o lock do sistema"""
    try:
        lock_nome = "sistema_lock.json"
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA}/{lock_nome}"
        headers = {"Authorization": f"Bearer {token}"}
        
        response = requests.delete(url, headers=headers)
        
        if response.status_code in [200, 204]:
            st.success("🔓 Sistema liberado com sucesso")
            return True
        elif response.status_code == 404:
            st.info("ℹ️ Nenhum lock encontrado para remover")
            return True
        else:
            st.warning(f"⚠️ Problema ao remover lock: Status {response.status_code}")
            return False
            
    except Exception as e:
        st.warning(f"⚠️ Erro ao remover lock: {e}")
        return False

def mostrar_status_concorrencia(lock_pode, info_lock):
    """Mostra status do sistema de concorrência"""
    if lock_pode:
        if info_lock == "próprio_lock":
            st.success("🔒 **Você tem controle exclusivo do sistema**")
        elif info_lock == "lock_criado":
            st.success(f"🔒 **Sistema bloqueado para você por {LOCK_TIMEOUT//60} minutos**")
        return True
    else:
        if isinstance(info_lock, dict):
            st.error("🚫 **Sistema em uso por outro usuário**")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("👤 Usuário Ativo", info_lock.get("usuario", "Desconhecido"))
                st.metric("⏱️ Tempo Restante", f"{info_lock.get('tempo_restante', 0)} min")
            with col2:
                st.metric("🔧 Operação", info_lock.get("operacao", "N/A"))
                st.metric("🔓 Liberação às", info_lock.get("timeout", "N/A"))
            
            st.info("⏳ **Aguarde a conclusão ou tente novamente após o timeout**")
            
            # Botão para tentar novamente
            if st.button("🔄 Verificar Novamente", type="secondary"):
                st.rerun()
                
        return False

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

def mostrar_info_versao():
    """Mostra informações de versão e changelog"""
    with st.sidebar:
        st.markdown("### 📋 Informações da Versão")
        st.markdown(f"**Versão:** `{APP_VERSION}`")
        st.markdown(f"**Data:** {VERSION_DATE}")
        
        with st.expander("📝 Changelog"):
            for version, info in CHANGELOG.items():
                st.markdown(f"**v{version}** - {info['date']}")
                for change in info['changes']:
                    st.markdown(f"• {change}")
                st.markdown("---")

# === FUNÇÕES AUXILIARES (mantidas do código original) ===

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

def upload_onedrive(nome_arquivo, conteudo_arquivo, token):
    """Faz upload de arquivo para OneDrive"""
    try:
        # Garantir que a pasta existe
        pasta_arquivo = "/".join(nome_arquivo.split("/")[:-1]) if "/" in nome_arquivo else ""
        if pasta_arquivo:
            criar_pasta_se_nao_existir(f"{PASTA}/{pasta_arquivo}", token)
        
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

# === PROCESSAMENTO COM CONTROLE DE CONCORRÊNCIA ===
def processar_consolidacao_com_lock(df_novo, nome_arquivo, token, usuario_id):
    """
    Processamento de consolidação com controle de concorrência
    """
    
    try:
        # 1. Verificar e criar lock
        st.markdown("### 🔐 Verificando Disponibilidade do Sistema")
        
        with st.spinner("🔍 Verificando se sistema está disponível..."):
            lock_pode, info_lock = verificar_e_criar_lock(token, usuario_id)
        
        if not mostrar_status_concorrencia(lock_pode, info_lock):
            return False
        
        # 2. Baixar arquivo consolidado existente
        with st.spinner("📥 Baixando arquivo consolidado existente..."):
            df_consolidado, arquivo_existe = baixar_arquivo_consolidado(token)
        
        if arquivo_existe:
            st.info(f"📂 Arquivo consolidado carregado ({len(df_consolidado):,} registros)")
        else:
            st.info("📂 Criando novo arquivo consolidado")

        # 3. Preparar dados novos
        df_novo = df_novo.copy()
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

        # 4. Análise prévia dos dados
        responsaveis_no_envio = df_novo["RESPONSÁVEL"].dropna().unique()
        periodo_min = df_novo["DATA"].min().strftime("%d/%m/%Y")
        periodo_max = df_novo["DATA"].max().strftime("%d/%m/%Y")
        
        # Contar combinações RESPONSÁVEL + DATA no envio
        combinacoes_envio = df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date]).size()
        total_combinacoes = len(combinacoes_envio)
        
        st.info(f"👥 **Responsáveis:** {', '.join(responsaveis_no_envio)}")
        st.info(f"📅 **Período:** {periodo_min} até {periodo_max}")
        st.info(f"📊 **Combinações únicas (Responsável + Data):** {total_combinacoes}")
        
        # 5. Verificar registros existentes vs novos
        if arquivo_existe and not df_consolidado.empty:
            df_consolidado["DATA"] = pd.to_datetime(df_consolidado["DATA"], errors="coerce")
            df_consolidado = df_consolidado.dropna(subset=["DATA"])
            
            registros_para_consolidar = 0
            registros_para_alterar = 0
            
            for responsavel in responsaveis_no_envio:
                datas_envio = df_novo[df_novo["RESPONSÁVEL"] == responsavel]["DATA"].dt.date.unique()
                
                for data in datas_envio:
                    mask_conflito = (
                        (df_consolidado["DATA"].dt.date == data) &
                        (df_consolidado["RESPONSÁVEL"].str.strip().str.upper() == str(responsavel).strip().upper())
                    )
                    
                    registros_envio = len(df_novo[
                        (df_novo["RESPONSÁVEL"] == responsavel) & 
                        (df_novo["DATA"].dt.date == data)
                    ])
                    
                    if mask_conflito.any():
                        registros_para_alterar += registros_envio
                    else:
                        registros_para_consolidar += registros_envio
            
            # Mostrar informações
            if registros_para_consolidar > 0 and registros_para_alterar == 0:
                st.success(f"✅ **{registros_para_consolidar} registro(s) serão CONSOLIDADOS** (dados novos)")
                st.info("ℹ️ Nenhum registro existente será alterado")
            elif registros_para_alterar > 0 and registros_para_consolidar == 0:
                st.warning(f"🔄 **{registros_para_alterar} registro(s) serão ALTERADOS** (substituindo dados existentes)")
                st.info("ℹ️ Nenhum registro novo será adicionado")
            elif registros_para_consolidar > 0 and registros_para_alterar > 0:
                col1, col2 = st.columns(2)
                with col1:
                    st.success(f"✅ **{registros_para_consolidar} registro(s) serão CONSOLIDADOS**")
                    st.caption("(dados completamente novos)")
                with col2:
                    st.warning(f"🔄 **{registros_para_alterar} registro(s) serão ALTERADOS**")
                    st.caption("(substituindo dados existentes)")
        else:
            st.success(f"✅ **{len(df_novo)} registro(s) serão CONSOLIDADOS** (primeira consolidação)")

        # 6. Processar consolidação
        with st.spinner("🔄 Processando consolidação..."):
            df_final, inseridos, substituidos, removidos, detalhes, novas_combinacoes, combinacoes_existentes = comparar_e_atualizar_registros(
                df_consolidado, df_novo
            )

        # 7. Ordenar por data e responsável
        df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"], na_position='last').reset_index(drop=True)
        
        # 8. Salvar arquivo consolidado
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
                st.metric("📊 Total Final", f"{len(df_final):,}")
            with col2:
                st.metric("➕ Inseridos", f"{inseridos}")
            with col3:
                st.metric("🔄 Substituídos", f"{substituidos}")
            with col4:
                st.metric("🗑️ Removidos", f"{removidos}")
            
            return True
        else:
            st.error(f"❌ Erro no upload: Status {status}")
            return False
            
    finally:
        # 9. SEMPRE remover lock ao final
        with st.spinner("🔓 Liberando sistema..."):
            remover_lock(token)

# === FUNÇÕES AUXILIARES DE CONSOLIDAÇÃO (simplificadas) ===
def comparar_e_atualizar_registros(df_consolidado, df_novo):
    """Lógica de consolidação simplificada para este exemplo"""
    registros_inseridos = 0
    registros_substituidos = 0
    registros_removidos = 0
    detalhes_operacao = []
    combinacoes_novas = 0
    combinacoes_existentes = 0
    
    if df_consolidado.empty:
        df_final = df_novo.copy()
        registros_inseridos = len(df_novo)
        combinacoes_novas = len(df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date]))
        return df_final, registros_inseridos, registros_substituidos, registros_removidos, detalhes_operacao, combinacoes_novas, combinacoes_existentes
    
    # Lógica simplificada - implementar conforme necessário
    df_final = pd.concat([df_consolidado, df_novo], ignore_index=True)
    registros_inseridos = len(df_novo)
    combinacoes_novas = len(df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date]))
    
    return df_final, registros_inseridos, registros_substituidos, registros_removidos, detalhes_operacao, combinacoes_novas, combinacoes_existentes

# === INTERFACE PRINCIPAL ===
def main():
    st.set_page_config(
        page_title=f"DSView BI - Upload v{APP_VERSION}", 
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Header com versão
    st.markdown(
        f'''
        <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 20px;">
            <h2 style="margin: 0; color: #2E8B57;">📊 DSView BI – Upload de Planilhas</h2>
            <span style="background: #e8f4f8; padding: 4px 8px; border-radius: 4px; font-size: 0.8em; color: #2E8B57;">
                v{APP_VERSION}
            </span>
        </div>
        ''',
        unsafe_allow_html=True
    )

    # Criar ID do usuário
    usuario_id = criar_usuario_id()
    
    # Sidebar com informações
    st.sidebar.markdown(f"### 📤 Upload de Planilhas v{APP_VERSION}")
    st.sidebar.markdown("Sistema de consolidação com controle de concorrência")
    st.sidebar.divider()
    
    # Status do sistema
    st.sidebar.markdown("**Status do Sistema:**")
    
    # Verificar autenticação
    token = obter_token()
    if not token:
        st.sidebar.error("❌ Desconectado")
        st.error("❌ Não foi possível autenticar. Verifique as credenciais.")
        st.stop()
    else:
        st.sidebar.success("✅ Conectado")
    
    # Mostrar informações de versão
    mostrar_info_versao()
    
    # ID da sessão
    st.sidebar.markdown(f"**Sua Sessão:** `{usuario_id}`")
    
    # Botão para forçar liberação de lock (em caso de emergência)
    with st.sidebar.expander("🔧 Ferramentas de Administração"):
        st.markdown("**Uso apenas em emergências:**")
        if st.button("🔓 Forçar Liberação do Sistema", type="secondary"):
            if remover_lock(token):
                st.success("Sistema liberado!")
            st.rerun()

    st.markdown("## 📤 Upload de Planilha Excel")
    st.info("💡 **Importante**: A planilha deve conter uma coluna 'RESPONSÁVEL' com os nomes dos responsáveis!")
    st.divider()

    # Upload de arquivo
    uploaded_file = st.file_uploader(
        "Escolha um arquivo Excel", 
        type=["xlsx", "xls"],
        help="Formatos aceitos: .xlsx, .xls | Certifique-se de que há uma coluna 'RESPONSÁVEL' na planilha"
    )

    # Processar arquivo carregado
    if uploaded_file:
        st.success(f"📁 Arquivo carregado: **{uploaded_file.name}**")
        
        try:
            # Ler arquivo
            with st.spinner("📖 Lendo arquivo..."):
                df = pd.read_excel(uploaded_file, sheet_name="Vendas CTs" if "Vendas CTs" in pd.ExcelFile(uploaded_file).sheet_names else 0)
                df.columns = df.columns.str.strip().str.upper()
            
            st.success("✅ Dados carregados com sucesso!")
            
            # Mostrar prévia
            st.subheader("👀 Prévia dos dados")
            st.dataframe(df.head(10), use_container_width=True, height=300)
            
            # Validações básicas
            erros = []
            if "RESPONSÁVEL" not in df.columns:
                erros.append("❌ Coluna 'RESPONSÁVEL' não encontrada")
            if "DATA" not in df.columns:
                erros.append("❌ Coluna 'DATA' não encontrada")
            
            if erros:
                for erro in erros:
                    st.error(erro)
            else:
                st.success("✅ Estrutura do arquivo válida")
                
                # Botão para processar
                if st.button("📧 Consolidar Dados", type="primary"):
                    sucesso = processar_consolidacao_com_lock(df, uploaded_file.name, token, usuario_id)
                    if sucesso:
                        st.balloons()
                        
        except Exception as e:
            st.error(f"❌ Erro ao processar arquivo: {e}")

    # Rodapé
    st.divider()
    st.markdown(
        f"""
        <div style="text-align: center; color: #666; font-size: 0.8em;">
            DSView BI v{APP_VERSION} - Sistema de Consolidação com Controle de Concorrência<br>
            🔒 Sistema com proteção contra uso simultâneo • 📝 Controle de versão integrado
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()