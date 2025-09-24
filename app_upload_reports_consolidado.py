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
import uuid
import time

# ===========================
# CONFIGURAÇÕES DE VERSÃO - ATUALIZADO v2.4.0
# ===========================
APP_VERSION = "2.4.0"
VERSION_DATE = "2025-09-16"
CHANGELOG = {
    "2.4.0": {
        "date": "2025-09-16",
        "changes": [
            "🎨 VISUAL MELHORADO: Interface moderna e responsiva",
            "📅 NOVO CAMPO: Data do último envio na planilha",
            "🎯 CSS organizado e componentes Streamlit otimizados",
            "📊 Dashboard visual aprimorado com métricas",
            "🔧 Melhor feedback visual durante processos",
            "🎨 Tema consistente e cores padronizadas",
            "📱 Layout responsivo para diferentes telas",
            "✨ Animações e transições suaves"
        ]
    },
    "2.3.0": {
        "date": "2025-08-20",
        "changes": [
            "🎯 CORREÇÃO CRÍTICA: Lógica de consolidação por MÊS/ANO",
            "🔧 Resolve problema de alteração de datas criando duplicatas",
            "📅 Consolidação agora agrupa por RESPONSÁVEL + PERÍODO MENSAL",
            "🛡️ Verificação de segurança aprimorada para períodos",
            "📊 Análise pré-consolidação atualizada para mês/ano",
            "🔍 Logs detalhados por período mensal",
            "⚡ Performance melhorada no agrupamento",
            "✅ Eliminação definitiva de inconsistências temporais"
        ]
    }
}

# ===========================
# ESTILOS CSS MELHORADOS
# ===========================
def aplicar_estilos_css():
    """Aplica estilos CSS customizados para melhorar a aparência"""
    st.markdown("""
    <style>
    /* Tema principal */
    :root {
        --primary-color: #2E8B57;
        --secondary-color: #20B2AA;
        --accent-color: #FFD700;
        --success-color: #32CD32;
        --warning-color: #FFA500;
        --error-color: #DC143C;
        --background-light: #F8F9FA;
        --text-dark: #2C3E50;
        --border-color: #E1E8ED;
    }
    
    /* Header principal */
    .main-header {
        background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        color: white;
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 700;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }
    
    .version-badge {
        background: rgba(255,255,255,0.2);
        padding: 0.5rem 1rem;
        border-radius: 25px;
        font-size: 0.9rem;
        font-weight: 600;
        backdrop-filter: blur(10px);
    }
    
    /* Cards de status */
    .status-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        border-left: 4px solid var(--primary-color);
        margin: 1rem 0;
        transition: transform 0.2s ease;
    }
    
    .status-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(0,0,0,0.12);
    }
    
    .status-card.success {
        border-left-color: var(--success-color);
        background: linear-gradient(135deg, #f0fff4, #ffffff);
    }
    
    .status-card.warning {
        border-left-color: var(--warning-color);
        background: linear-gradient(135deg, #fffaf0, #ffffff);
    }
    
    .status-card.error {
        border-left-color: var(--error-color);
        background: linear-gradient(135deg, #fff0f0, #ffffff);
    }
    
    /* Métricas melhoradas */
    .metric-container {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        text-align: center;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        border-top: 3px solid var(--primary-color);
        transition: all 0.3s ease;
    }
    
    .metric-container:hover {
        transform: translateY(-3px);
        box-shadow: 0 6px 25px rgba(0,0,0,0.15);
    }
    
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--primary-color);
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-size: 0.9rem;
        color: var(--text-dark);
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Botões melhorados */
    .stButton > button {
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.3s ease;
        border: none;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    
    /* Progress bar customizada */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
        border-radius: 10px;
    }
    
    /* Sidebar melhorada */
    .css-1d391kg {
        background: linear-gradient(180deg, var(--background-light), white);
    }
    
    /* Upload area melhorada */
    .uploadedFile {
        border: 2px dashed var(--primary-color);
        border-radius: 12px;
        padding: 2rem;
        text-align: center;
        background: var(--background-light);
        transition: all 0.3s ease;
    }
    
    .uploadedFile:hover {
        border-color: var(--secondary-color);
        background: white;
    }
    
    /* Tabelas melhoradas */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
    }
    
    /* Alertas customizados */
    .custom-alert {
        padding: 1rem 1.5rem;
        border-radius: 8px;
        margin: 1rem 0;
        border-left: 4px solid;
        font-weight: 500;
    }
    
    .custom-alert.info {
        background: #e3f2fd;
        border-left-color: #2196f3;
        color: #0d47a1;
    }
    
    .custom-alert.success {
        background: #e8f5e8;
        border-left-color: #4caf50;
        color: #1b5e20;
    }
    
    .custom-alert.warning {
        background: #fff3e0;
        border-left-color: #ff9800;
        color: #e65100;
    }
    
    .custom-alert.error {
        background: #ffebee;
        border-left-color: #f44336;
        color: #b71c1c;
    }
    
    /* Footer melhorado */
    .footer {
        background: var(--background-light);
        padding: 2rem;
        border-radius: 12px;
        text-align: center;
        margin-top: 3rem;
        border-top: 3px solid var(--primary-color);
    }
    
    /* Animações */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    .fade-in {
        animation: fadeIn 0.6s ease-out;
    }
    
    /* Responsividade */
    @media (max-width: 768px) {
        .main-header h1 {
            font-size: 2rem;
        }
        
        .metric-value {
            font-size: 2rem;
        }
        
        .status-card {
            padding: 1rem;
        }
    }
    </style>
    """, unsafe_allow_html=True)

# ===========================
# CONFIGURAÇÃO DE LOGGING
# ===========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===========================
# CREDENCIAIS VIA ST.SECRETS
# ===========================
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

# ===========================
# CONFIGURAÇÃO DE PASTAS
# ===========================
PASTA_CONSOLIDADO = "Documentos Compartilhados/LimparAuto/FontedeDados"
PASTA_ENVIOS_BACKUPS = "Documentos Compartilhados/PlanilhasEnviadas_Backups/LimparAuto"
PASTA = PASTA_CONSOLIDADO

# ===========================
# CONFIGURAÇÃO DO SISTEMA DE LOCK
# ===========================
ARQUIVO_LOCK = "sistema_lock.json"
TIMEOUT_LOCK_MINUTOS = 10

# ===========================
# AUTENTICAÇÃO
# ===========================
@st.cache_data(ttl=3300)
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

# ===========================
# SISTEMA DE LOCK
# ===========================
def gerar_id_sessao():
    """Gera um ID único para a sessão atual"""
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())[:8]
    return st.session_state.session_id

def verificar_lock_existente(token):
    """Verifica se existe um lock ativo no sistema"""
    try:
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA_CONSOLIDADO}/{ARQUIVO_LOCK}:/content"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            lock_data = response.json()
            timestamp_lock = datetime.fromisoformat(lock_data['timestamp'])
            agora = datetime.now()
            
            if agora - timestamp_lock > timedelta(minutes=TIMEOUT_LOCK_MINUTOS):
                logger.info(f"Lock expirado removido automaticamente. Era de {timestamp_lock}")
                remover_lock(token, force=True)
                return False, None
            
            return True, lock_data
        
        elif response.status_code == 404:
            return False, None
        else:
            logger.warning(f"Erro ao verificar lock: {response.status_code}")
            return False, None
            
    except Exception as e:
        logger.error(f"Erro ao verificar lock: {e}")
        return False, None

def criar_lock(token, operacao="Consolidação de dados"):
    """Cria um lock para bloquear outras operações"""
    try:
        session_id = gerar_id_sessao()
        
        lock_data = {
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "operacao": operacao,
            "status": "EM_ANDAMENTO",
            "app_version": APP_VERSION
        }
        
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA_CONSOLIDADO}/{ARQUIVO_LOCK}:/content"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.put(url, headers=headers, data=json.dumps(lock_data))
        
        if response.status_code in [200, 201]:
            logger.info(f"Lock criado com sucesso. Session ID: {session_id}")
            return True, session_id
        else:
            logger.error(f"Erro ao criar lock: {response.status_code}")
            return False, None
            
    except Exception as e:
        logger.error(f"Erro ao criar lock: {e}")
        return False, None

def remover_lock(token, session_id=None, force=False):
    """Remove o lock do sistema"""
    try:
        if not force and session_id:
            lock_existe, lock_data = verificar_lock_existente(token)
            if lock_existe and lock_data.get('session_id') != session_id:
                logger.warning("Tentativa de remover lock de outra sessão!")
                return False
        
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA_CONSOLIDADO}/{ARQUIVO_LOCK}"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.delete(url, headers=headers)
        
        if response.status_code in [200, 204]:
            logger.info("Lock removido com sucesso")
            return True
        elif response.status_code == 404:
            return True
        else:
            logger.error(f"Erro ao remover lock: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao remover lock: {e}")
        return False

def atualizar_status_lock(token, session_id, novo_status, detalhes=None):
    """Atualiza o status do lock durante o processo"""
    try:
        lock_existe, lock_data = verificar_lock_existente(token)
        
        if not lock_existe or lock_data.get('session_id') != session_id:
            logger.warning("Lock não existe ou não pertence a esta sessão")
            return False
        
        lock_data['status'] = novo_status
        lock_data['ultima_atualizacao'] = datetime.now().isoformat()
        
        if detalhes:
            lock_data['detalhes'] = detalhes
        
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA_CONSOLIDADO}/{ARQUIVO_LOCK}:/content"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.put(url, headers=headers, data=json.dumps(lock_data))
        return response.status_code in [200, 201]
        
    except Exception as e:
        logger.error(f"Erro ao atualizar status do lock: {e}")
        return False

def exibir_status_sistema(token):
    """Exibe o status atual do sistema de lock com visual melhorado"""
    lock_existe, lock_data = verificar_lock_existente(token)
    
    if lock_existe:
        timestamp_inicio = datetime.fromisoformat(lock_data['timestamp'])
        duracao = datetime.now() - timestamp_inicio
        
        # Card de status ocupado
        st.markdown("""
        <div class="status-card error">
            <h3>🔒 Sistema Ocupado</h3>
            <p>Outro usuário está enviando dados no momento</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Métricas do processo
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div class="metric-container">
                <div class="metric-value">{int(duracao.total_seconds()//60)}</div>
                <div class="metric-label">Minutos Ativo</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            tempo_limite = timestamp_inicio + timedelta(minutes=TIMEOUT_LOCK_MINUTOS)
            tempo_restante = tempo_limite - datetime.now()
            minutos_restantes = max(0, int(tempo_restante.total_seconds()//60))
            
            st.markdown(f"""
            <div class="metric-container">
                <div class="metric-value">{minutos_restantes}</div>
                <div class="metric-label">Min. Restantes</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            status_display = lock_data.get('status', 'N/A')
            st.markdown(f"""
            <div class="metric-container">
                <div class="metric-label">Status</div>
                <div style="font-size: 1.2rem; font-weight: 600; color: var(--warning-color);">{status_display}</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Detalhes em expander
        with st.expander("ℹ️ Detalhes do processo em andamento"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"**Operação:** {lock_data.get('operacao', 'N/A')}")
                st.info(f"**Início:** {timestamp_inicio.strftime('%H:%M:%S')}")
                
            with col2:
                if 'detalhes' in lock_data:
                    st.info(f"**Detalhes:** {lock_data['detalhes']}")
                    
                session_id_display = lock_data.get('session_id', 'N/A')[:8]
                st.caption(f"Session ID: {session_id_display}")
        
        if tempo_restante.total_seconds() < 0:
            if st.button("🆘 Liberar Sistema (Forçar)", type="secondary"):
                if remover_lock(token, force=True):
                    st.success("✅ Sistema liberado com sucesso!")
                    st.rerun()
                else:
                    st.error("❌ Erro ao liberar sistema")
        
        return True
    else:
        st.markdown("""
        <div class="status-card success">
            <h3>✅ Sistema Disponível</h3>
            <p>Você pode enviar sua planilha agora</p>
        </div>
        """, unsafe_allow_html=True)
        return False

# ===========================
# FUNÇÕES AUXILIARES
# ===========================
def criar_pasta_se_nao_existir(caminho_pasta, token):
    """Cria pasta no OneDrive se não existir"""
    try:
        partes = caminho_pasta.split('/')
        caminho_atual = ""
        
        for parte in partes:
            if not parte:
                continue
                
            caminho_anterior = caminho_atual
            caminho_atual = f"{caminho_atual}/{parte}" if caminho_atual else parte
            
            url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{caminho_atual}"
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, headers=headers)
            
            if response.status_code == 404:
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

def upload_onedrive(nome_arquivo, conteudo_arquivo, token, tipo_arquivo="consolidado"):
    """Faz upload de arquivo para OneDrive"""
    try:
        if tipo_arquivo == "consolidado":
            pasta_base = PASTA_CONSOLIDADO
        elif tipo_arquivo in ["enviado", "backup"]:
            pasta_base = PASTA_ENVIOS_BACKUPS
        else:
            pasta_base = PASTA_CONSOLIDADO
        
        pasta_arquivo = "/".join(nome_arquivo.split("/")[:-1]) if "/" in nome_arquivo else ""
        if pasta_arquivo:
            criar_pasta_se_nao_existir(f"{pasta_base}/{pasta_arquivo}", token)
        
        if tipo_arquivo == "consolidado" and "/" not in nome_arquivo:
            mover_arquivo_existente(nome_arquivo, token, pasta_base)
        
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{pasta_base}/{nome_arquivo}:/content"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream"
        }
        response = requests.put(url, headers=headers, data=conteudo_arquivo)
        
        return response.status_code in [200, 201], response.status_code, response.text
        
    except Exception as e:
        logger.error(f"Erro no upload: {e}")
        return False, 500, f"Erro interno: {str(e)}"

def mover_arquivo_existente(nome_arquivo, token, pasta_base=None):
    """Move arquivo existente para backup antes de substituir"""
    try:
        if pasta_base is None:
            pasta_base = PASTA_CONSOLIDADO
            
        url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{pasta_base}/{nome_arquivo}"
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

# ===========================
# VALIDAÇÃO DE DATAS
# ===========================
def validar_datas_detalhadamente(df):
    """Validação detalhada de datas"""
    problemas = []
    
    logger.info(f"🔍 Iniciando validação detalhada de {len(df)} registros...")
    
    for idx, row in df.iterrows():
        linha_excel = idx + 2
        valor_original = row["DATA"]
        responsavel = row.get("RESPONSÁVEL", "N/A")
        
        problema_encontrado = None
        tipo_problema = None
        
        if pd.isna(valor_original) or str(valor_original).strip() == "":
            problema_encontrado = "Data vazia ou nula"
            tipo_problema = "VAZIO"
        else:
            try:
                data_convertida = pd.to_datetime(valor_original, errors='raise')
                hoje = datetime.now()
                
                if data_convertida > hoje + pd.Timedelta(days=730):
                    problema_encontrado = f"Data muito distante no futuro: {data_convertida.strftime('%d/%m/%Y')}"
                    tipo_problema = "FUTURO"
                elif data_convertida < pd.Timestamp('2020-01-01'):
                    problema_encontrado = f"Data muito antiga: {data_convertida.strftime('%d/%m/%Y')}"
                    tipo_problema = "ANTIGA"
                elif data_convertida > hoje:
                    problema_encontrado = f"Data no futuro: {data_convertida.strftime('%d/%m/%Y')}"
                    tipo_problema = "FUTURO"
                    
            except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime) as e:
                if "day is out of range for month" in str(e) or "month must be in 1..12" in str(e):
                    problema_encontrado = f"Data impossível: {valor_original}"
                    tipo_problema = "IMPOSSÍVEL"
                else:
                    problema_encontrado = f"Formato inválido: {valor_original}"
                    tipo_problema = "FORMATO"
        
        if problema_encontrado:
            problemas.append({
                "Linha Excel": linha_excel,
                "Responsável": responsavel,
                "Valor Original": valor_original,
                "Problema": problema_encontrado,
                "Tipo Problema": tipo_problema
            })
            
            logger.warning(f"❌ Linha {linha_excel}: {problema_encontrado} (Responsável: {responsavel})")
    
    if problemas:
        logger.error(f"❌ TOTAL DE PROBLEMAS ENCONTRADOS: {len(problemas)}")
        
        tipos_problema = {}
        for problema in problemas:
            tipo = problema["Tipo Problema"]
            tipos_problema[tipo] = tipos_problema.get(tipo, 0) + 1
        
        logger.error(f"📊 Problemas por tipo: {tipos_problema}")
    else:
        logger.info("✅ Todas as datas estão válidas!")
    
    return problemas

def exibir_problemas_datas(problemas_datas):
    """Exibe problemas de datas com visual melhorado"""
    if not problemas_datas:
        return
    
    st.markdown("""
    <div class="custom-alert error">
        <h4>❌ Problemas de Data Encontrados</h4>
        <p>É obrigatório corrigir TODOS os problemas antes de enviar</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Estatísticas dos problemas
    tipos_problema = {}
    for problema in problemas_datas:
        tipo = problema["Tipo Problema"]
        tipos_problema[tipo] = tipos_problema.get(tipo, 0) + 1
    
    # Exibir métricas dos problemas
    cols = st.columns(len(tipos_problema))
    emoji_map = {
        "VAZIO": "🔴",
        "FORMATO": "🟠", 
        "IMPOSSÍVEL": "🟣",
        "FUTURO": "🟡",
        "ANTIGA": "🟤"
    }
    
    for i, (tipo, qtd) in enumerate(tipos_problema.items()):
        with cols[i]:
            emoji = emoji_map.get(tipo, "❌")
            st.markdown(f"""
            <div class="metric-container">
                <div class="metric-value">{qtd}</div>
                <div class="metric-label">{emoji} {tipo}</div>
            </div>
            """, unsafe_allow_html=True)
    
    # Tabela de problemas
    df_problemas = pd.DataFrame(problemas_datas)
    max_linhas_exibir = 50
    
    if len(df_problemas) > max_linhas_exibir:
        df_problemas_exibir = df_problemas.head(max_linhas_exibir)
        st.warning(f"⚠️ **Exibindo apenas as primeiras {max_linhas_exibir} linhas.** Total de problemas: {len(df_problemas)}")
    else:
        df_problemas_exibir = df_problemas
    
    st.dataframe(
        df_problemas_exibir,
        use_container_width=True,
        hide_index=True,
        height=400
    )

def normalizar_texto(texto):
    """Normaliza strings: remove espaços extras, converte para maiúsculas e remove acentos."""
    if pd.isna(texto) or not isinstance(texto, str):
        return texto
    texto = texto.strip().upper()
    texto = ''.join(c for c in texto if c.isalnum() or c.isspace())
    return unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')

def validar_dados_enviados(df):
    """Validação super rigorosa dos dados enviados"""
    erros = []
    avisos = []
    linhas_invalidas_detalhes = []
    
    if df.empty:
        erros.append("❌ A planilha está vazia")
        return erros, avisos, linhas_invalidas_detalhes
    
    # Validação de campos específicos
    campos_para_validar = ["GRUPO", "CONCESSIONÁRIA", "LOJA", "MARCA", "UF", "RESPONSÁVEL", "CONSULTORES"]
    
    for campo in campos_para_validar:
        if campo not in df.columns:
            erros.append(f"⚠️ A planilha deve conter uma coluna \'{campo}\'")
            avisos.append(f"📋 Certifique-se de que sua planilha tenha uma coluna chamada \'{campo}\'")
            continue
        
        # Normalizar a coluna para facilitar a comparação e evitar inconsistências
        df[f"_{campo}_NORMALIZADO"] = df[campo].apply(normalizar_texto)
        
        # Verificar valores vazios após normalização
        if df[f"_{campo}_NORMALIZADO"].isnull().any():
            erros.append(f"❌ Coluna \'{campo}\' contém valores vazios ou inválidos após normalização.")
            
        # Verificar inconsistências de capitalização/espaços
        valores_originais = df[campo].dropna().unique()
        valores_normalizados = df[f"_{campo}_NORMALIZADO"].dropna().unique()
        
        if len(valores_originais) != len(valores_normalizados):
            # Isso indica que há valores que são diferentes na forma original, mas iguais após normalização
            # Ex: 'Nome' e 'nome', ou 'Nome ' e 'Nome'
            inconsistencias_encontradas = []
            for val_norm in valores_normalizados:
                originais_para_norm = [v for v in valores_originais if normalizar_texto(v) == val_norm]
                if len(originais_para_norm) > 1:
                    inconsistencias_encontradas.append(f"'{val_norm}' (originalmente: {', '.join(originais_para_norm)})")
            
            if inconsistencias_encontradas:
                erros.append(f"❌ Inconsistências de capitalização/espaços na coluna \'{campo}\': {'; '.join(inconsistencias_encontradas)}")
                avisos.append(f"💡 Considere padronizar os valores na coluna \'{campo}\' para evitar problemas.")

    if "RESPONSÁVEL" not in df.columns:
        erros.append("⚠️ A planilha deve conter uma coluna 'RESPONSÁVEL'")
        avisos.append("📋 Certifique-se de que sua planilha tenha uma coluna chamada 'RESPONSÁVEL'")
    else:
        responsaveis_validos = df["RESPONSÁVEL"].notna().sum()
        if responsaveis_validos == 0:
            erros.append("❌ Nenhum responsável válido encontrado na coluna 'RESPONSÁVEL'")
        else:
            responsaveis_unicos = df["RESPONSÁVEL"].dropna().unique()
            if len(responsaveis_unicos) > 0:
                avisos.append(f"👥 Responsáveis encontrados: {', '.join(responsaveis_unicos[:5])}")
                if len(responsaveis_unicos) > 5:
                    avisos.append(f"... e mais {len(responsaveis_unicos) - 5} responsáveis")
    
    if "DATA" not in df.columns:
        erros.append("⚠️ A planilha deve conter uma coluna 'DATA'")
        avisos.append("📋 Lembre-se: o arquivo deve ter uma aba chamada 'Vendas CTs' com as colunas 'DATA' e 'RESPONSÁVEL'")
    else:
        problemas_datas = validar_datas_detalhadamente(df)
        
        if problemas_datas:
            erros.append(f"❌ {len(problemas_datas)} problemas de data encontrados - CONSOLIDAÇÃO BLOQUEADA")
            erros.append("🔧 É OBRIGATÓRIO corrigir TODOS os problemas antes de enviar")
            erros.append("📋 Revise sua planilha e corrija todas as datas inválidas")
            
            linhas_invalidas_detalhes = problemas_datas
            
        else:
            avisos.append("✅ Todas as datas estão válidas e consistentes!")
    
    if not df.empty and "DATA" in df.columns:
        df_temp = df.copy()
        df_temp["DATA"] = pd.to_datetime(df_temp["DATA"], errors="coerce")
        df_temp = df_temp.dropna(subset=["DATA"])
        
        if not df_temp.empty:
            duplicatas = df_temp.duplicated(subset=["DATA"], keep=False).sum()
            if duplicatas > 0:
                avisos.append(f"⚠️ {duplicatas} linhas com datas duplicadas na planilha")
    
    return erros, avisos, linhas_invalidas_detalhes

# ===========================
# FUNÇÕES DE CONSOLIDAÇÃO MELHORADAS v2.4.0
# ===========================
def baixar_arquivo_consolidado(token):
    """Baixa o arquivo consolidado existente"""
    consolidado_nome = "Reports_Geral_Consolidado.xlsx"
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA_CONSOLIDADO}/{consolidado_nome}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            df_consolidado = pd.read_excel(BytesIO(response.content))
            df_consolidado.columns = df_consolidado.columns.str.strip().str.upper()
            
            logger.info(f"✅ Arquivo consolidado baixado: {len(df_consolidado)} registros")
            if not df_consolidado.empty:
                responsaveis_existentes = df_consolidado['RESPONSÁVEL'].dropna().unique()
                logger.info(f"📊 Responsáveis no consolidado: {responsaveis_existentes}")
            
            return df_consolidado, True
        else:
            logger.info("📄 Arquivo consolidado não existe - será criado novo")
            return pd.DataFrame(), False
            
    except Exception as e:
        logger.error(f"Erro ao baixar arquivo consolidado: {e}")
        return pd.DataFrame(), False

def adicionar_data_ultimo_envio(df_final, responsaveis_atualizados):
    """Adiciona/atualiza a coluna DATA_ULTIMO_ENVIO para os responsáveis que foram atualizados"""
    try:
        # Garantir que a coluna existe
        if 'DATA_ULTIMO_ENVIO' not in df_final.columns:
            df_final['DATA_ULTIMO_ENVIO'] = pd.NaT
            logger.info("➕ Coluna 'DATA_ULTIMO_ENVIO' criada")
        
        # Atualizar apenas os responsáveis que foram modificados neste envio
        data_atual = datetime.now()
        
        for responsavel in responsaveis_atualizados:
            mask = df_final['RESPONSÁVEL'].astype(str).str.strip().str.upper() == str(responsavel).strip().upper()
            df_final.loc[mask, 'DATA_ULTIMO_ENVIO'] = data_atual
            
            registros_atualizados = mask.sum()
            logger.info(f"📅 Data do último envio atualizada para '{responsavel}': {registros_atualizados} registros")
        
        return df_final
        
    except Exception as e:
        logger.error(f"Erro ao adicionar data do último envio: {e}")
        return df_final

def verificar_seguranca_consolidacao_v2(df_consolidado, df_novo, df_final):
    """Verificação de segurança crítica - versão corrigida para mês/ano"""
    try:
        responsaveis_antes = set(df_consolidado['RESPONSÁVEL'].dropna().astype(str).str.strip().str.upper().unique()) if not df_consolidado.empty else set()
        responsaveis_novos = set(df_novo['RESPONSÁVEL'].dropna().astype(str).str.strip().str.upper().unique())
        responsaveis_depois = set(df_final['RESPONSÁVEL'].dropna().astype(str).str.strip().str.upper().unique())
        
        logger.info(f"🛡️ VERIFICAÇÃO DE SEGURANÇA v2.4.0:")
        logger.info(f"   Responsáveis ANTES: {responsaveis_antes}")
        logger.info(f"   Responsáveis NOVOS: {responsaveis_novos}")
        logger.info(f"   Responsáveis DEPOIS: {responsaveis_depois}")
        
        # Verificar se algum responsável foi perdido completamente
        responsaveis_esperados = responsaveis_antes.union(responsaveis_novos)
        responsaveis_perdidos = responsaveis_esperados - responsaveis_depois
        
        if responsaveis_perdidos:
            error_msg = f"Responsáveis perdidos durante consolidação: {', '.join(responsaveis_perdidos)}"
            logger.error(f"❌ ERRO CRÍTICO: {error_msg}")
            return False, error_msg
        
        # Verificação adicional: responsáveis que não estão sendo atualizados não devem desaparecer
        for resp in responsaveis_antes:
            if resp not in responsaveis_novos:
                if resp not in responsaveis_depois:
                    error_msg = f"Responsável '{resp}' foi removido sem justificativa"
                    logger.error(f"❌ ERRO: {error_msg}")
                    return False, error_msg
        
        logger.info(f"✅ VERIFICAÇÃO DE SEGURANÇA PASSOU!")
        logger.info(f"   Total antes: {len(responsaveis_antes)} responsáveis")
        logger.info(f"   Total novos: {len(responsaveis_novos)} responsáveis") 
        logger.info(f"   Total depois: {len(responsaveis_depois)} responsáveis")
        
        return True, f"Verificação passou: {len(responsaveis_depois)} responsáveis mantidos"
        
    except Exception as e:
        error_msg = f"Erro durante verificação de segurança: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return False, error_msg

def comparar_e_atualizar_registros_v2(df_consolidado, df_novo):
    """
    Lógica de consolidação corrigida - v2.4.0
    Consolida por RESPONSÁVEL + MÊS/ANO para evitar problemas com alterações de data
    """
    registros_inseridos = 0
    registros_substituidos = 0
    registros_removidos = 0
    detalhes_operacao = []
    combinacoes_novas = 0
    combinacoes_existentes = 0
    responsaveis_atualizados = set()
    
    logger.info(f"🔧 INICIANDO CONSOLIDAÇÃO v2.4.0:")
    logger.info(f"   Consolidado atual: {len(df_consolidado)} registros")
    logger.info(f"   Novo arquivo: {len(df_novo)} registros")
    
    if df_consolidado.empty:
        df_final = df_novo.copy()
        registros_inseridos = len(df_novo)
        
        # Adicionar todos os responsáveis como atualizados
        responsaveis_atualizados = set(df_novo['RESPONSÁVEL'].dropna().astype(str).str.strip().str.upper().unique())
        
        # Criar combinações únicas por mês/ano
        df_temp = df_novo.copy()
        df_temp['mes_ano'] = df_temp['DATA'].dt.to_period('M')
        combinacoes_unicas = df_temp.groupby(['RESPONSÁVEL', 'mes_ano']).size()
        combinacoes_novas = len(combinacoes_unicas)
        
        logger.info(f"✅ PRIMEIRA CONSOLIDAÇÃO: {registros_inseridos} registros inseridos")
        
        for _, row in df_novo.iterrows():
            detalhes_operacao.append({
                "Operação": "INSERIDO",
                "Responsável": row["RESPONSÁVEL"],
                "Mês/Ano": row["DATA"].strftime("%m/%Y"),
                "Data": row["DATA"].strftime("%d/%m/%Y"),
                "Motivo": "Primeira consolidação - arquivo vazio"
            })
        
        # Adicionar data do último envio
        df_final = adicionar_data_ultimo_envio(df_final, responsaveis_atualizados)
        
        return df_final, registros_inseridos, registros_substituidos, registros_removidos, detalhes_operacao, combinacoes_novas, combinacoes_existentes
    
    # Garantir que as colunas existem no consolidado
    colunas = df_novo.columns.tolist()
    for col in colunas:
        if col not in df_consolidado.columns:
            df_consolidado[col] = None
            logger.info(f"➕ Coluna '{col}' adicionada ao consolidado")
    
    # Começar com uma CÓPIA COMPLETA do consolidado
    df_final = df_consolidado.copy()
    registros_inicial = len(df_final)
    
    # Adicionar colunas auxiliares para agrupamento por mês/ano
    df_novo_temp = df_novo.copy()
    df_novo_temp['mes_ano'] = df_novo_temp['DATA'].dt.to_period('M')
    
    df_final_temp = df_final.copy()
    df_final_temp['mes_ano'] = df_final_temp['DATA'].dt.to_period('M')
    
    logger.info(f"📋 Estado inicial do consolidado:")
    if not df_final.empty:
        responsaveis_iniciais = df_final['RESPONSÁVEL'].dropna().unique()
        logger.info(f"   Responsáveis: {responsaveis_iniciais}")
        logger.info(f"   Total de registros: {len(df_final)}")
    
    # Agrupar registros novos por RESPONSÁVEL e MÊS/ANO
    grupos_novos = df_novo_temp.groupby(['RESPONSÁVEL', 'mes_ano'])
    
    logger.info(f"📊 Processando {len(grupos_novos)} combinações únicas de Responsável+Mês/Ano")
    
    for (responsavel, periodo_grupo), grupo_df in grupos_novos:
        if pd.isna(responsavel) or str(responsavel).strip() == '':
            logger.warning(f"⚠️ Pulando responsável inválido: {responsavel}")
            continue
        
        # Adicionar responsável à lista de atualizados
        responsaveis_atualizados.add(str(responsavel).strip().upper())
        
        logger.info(f"🔍 Processando: '{responsavel}' em {periodo_grupo} ({len(grupo_df)} registros)")
        
        # Buscar registros existentes APENAS para este responsável e período ESPECÍFICOS
        mask_existente = (
            (df_final_temp["mes_ano"] == periodo_grupo) &
            (df_final_temp["RESPONSÁVEL"].astype(str).str.strip().str.upper() == str(responsavel).strip().upper())
        )
        
        registros_existentes = df_final[mask_existente]
        total_antes_operacao = len(df_final)
        
        logger.info(f"   📋 Encontrados {len(registros_existentes)} registros existentes para esta combinação")
        
        if not registros_existentes.empty:
            # SUBSTITUIÇÃO APENAS DA COMBINAÇÃO ESPECÍFICA (RESPONSÁVEL + MÊS/ANO)
            num_removidos = len(registros_existentes)
            
            logger.info(f"   🔄 SUBSTITUIÇÃO: Removendo {num_removidos} registros antigos do período {periodo_grupo}")
            
            # Remove APENAS os registros dessa combinação específica
            df_final = df_final[~mask_existente]
            
            # Atualizar df_final_temp também
            df_final_temp = df_final_temp[~mask_existente]
            
            total_depois_remocao = len(df_final)
            registros_removidos += num_removidos
            combinacoes_existentes += 1
            
            logger.info(f"   ✅ Removidos {num_removidos} registros. Total: {total_antes_operacao} -> {total_depois_remocao}")
            
            # Verificação de segurança na remoção
            if total_depois_remocao != (total_antes_operacao - num_removidos):
                logger.error(f"❌ ERRO NA REMOÇÃO! Esperado: {total_antes_operacao - num_removidos}, Atual: {total_depois_remocao}")
            
            # Adicionar detalhes da remoção
            detalhes_operacao.append({
                "Operação": "REMOVIDO",
                "Responsável": responsavel,
                "Mês/Ano": periodo_grupo.strftime("%m/%Y"),
                "Data": f"Todo o período {periodo_grupo}",
                "Motivo": f"Substituição: {num_removidos} registro(s) antigo(s) removido(s)"
            })
            
            registros_substituidos += len(grupo_df)
            operacao_tipo = "SUBSTITUÍDO"
            motivo = f"Substituição completa do período: {len(grupo_df)} novo(s) registro(s)"
            
        else:
            # INSERÇÃO DE NOVOS DADOS
            logger.info(f"   ➕ NOVA COMBINAÇÃO: Adicionando {len(grupo_df)} registros para {periodo_grupo}")
            registros_inseridos += len(grupo_df)
            combinacoes_novas += 1
            operacao_tipo = "INSERIDO"
            motivo = f"Nova combinação: {len(grupo_df)} registro(s) inserido(s)"
        
        # Inserir novos registros (tanto para inserção quanto substituição)
        total_antes_insercao = len(df_final)
        
        # Remover coluna auxiliar antes de concatenar
        grupo_para_inserir = grupo_df.drop(columns=['mes_ano'], errors='ignore')
        df_final = pd.concat([df_final, grupo_para_inserir], ignore_index=True)
        
        # Atualizar df_final_temp também
        df_final_temp = pd.concat([df_final_temp, grupo_df], ignore_index=True)
        
        total_depois_insercao = len(df_final)
        
        logger.info(f"   ✅ Inseridos {len(grupo_df)} registros. Total: {total_antes_insercao} -> {total_depois_insercao}")
        
        # Adicionar detalhes da operação
        detalhes_operacao.append({
            "Operação": operacao_tipo,
            "Responsável": responsavel,
            "Mês/Ano": periodo_grupo.strftime("%m/%Y"),
            "Data": f"Período {periodo_grupo}",
            "Motivo": motivo
        })
    
    # Adicionar data do último envio para os responsáveis atualizados
    df_final = adicionar_data_ultimo_envio(df_final, responsaveis_atualizados)
    
    logger.info(f"🎯 CONSOLIDAÇÃO FINALIZADA:")
    logger.info(f"   Registros inseridos: {registros_inseridos}")
    logger.info(f"   Registros substituídos: {registros_substituidos}")
    logger.info(f"   Registros removidos: {registros_removidos}")
    logger.info(f"   Novas combinações: {combinacoes_novas}")
    logger.info(f"   Combinações existentes: {combinacoes_existentes}")
    logger.info(f"   Responsáveis atualizados: {responsaveis_atualizados}")
    logger.info(f"   Total final: {len(df_final)} registros")
    
    return df_final, registros_inseridos, registros_substituidos, registros_removidos, detalhes_operacao, combinacoes_novas, combinacoes_existentes

def salvar_arquivo_enviado(df_novo, nome_arquivo_original, token):
    """Salva uma cópia do arquivo enviado na pasta de backups"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%Hh%M")
        nome_base = nome_arquivo_original.replace(".xlsx", "").replace(".xls", "")
        nome_arquivo_backup = f"{nome_base}_enviado_{timestamp}.xlsx"
        
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_novo.to_excel(writer, index=False, sheet_name="Vendas CTs")
        buffer.seek(0)
        
        sucesso, status_code, resposta = upload_onedrive(nome_arquivo_backup, buffer.read(), token, "backup")
        
        if sucesso:
            logger.info(f"💾 Arquivo enviado salvo como backup: {nome_arquivo_backup}")
        else:
            logger.warning(f"⚠️ Não foi possível salvar backup do arquivo enviado: {status_code}")
            
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo enviado: {e}")

def analise_pre_consolidacao_v2(df_consolidado, df_novo):
    """Análise pré-consolidação com visual melhorado"""
    try:
        st.markdown("### 📊 Análise Pré-Consolidação")
        
        # Preparar dados para análise
        df_novo_temp = df_novo.copy()
        df_novo_temp['mes_ano'] = df_novo_temp['DATA'].dt.to_period('M')
        
        responsaveis_novos = set(df_novo['RESPONSÁVEL'].dropna().astype(str).str.strip().str.upper().unique())
        
        if not df_consolidado.empty:
            df_consolidado_temp = df_consolidado.copy()
            df_consolidado_temp['mes_ano'] = df_consolidado_temp['DATA'].dt.to_period('M')
            responsaveis_existentes = set(df_consolidado['RESPONSÁVEL'].dropna().astype(str).str.strip().str.upper().unique())
        else:
            responsaveis_existentes = set()
        
        # Análise de combinações
        combinacoes_novas = []
        combinacoes_existentes = []
        
        grupos_novos = df_novo_temp.groupby(['RESPONSÁVEL', 'mes_ano'])
        
        for (responsavel, periodo), grupo in grupos_novos:
            if pd.isna(responsavel):
                continue
                
            responsavel_upper = str(responsavel).strip().upper()
            
            if not df_consolidado.empty:
                mask_existente = (
                    (df_consolidado_temp["mes_ano"] == periodo) &
                    (df_consolidado_temp["RESPONSÁVEL"].astype(str).str.strip().str.upper() == responsavel_upper)
                )
                
                if mask_existente.any():
                    combinacoes_existentes.append({
                        "Responsável": responsavel,
                        "Período": periodo.strftime("%m/%Y"),
                        "Novos Registros": len(grupo),
                        "Registros Existentes": mask_existente.sum()
                    })
                else:
                    combinacoes_novas.append({
                        "Responsável": responsavel,
                        "Período": periodo.strftime("%m/%Y"),
                        "Registros": len(grupo)
                    })
            else:
                combinacoes_novas.append({
                    "Responsável": responsavel,
                    "Período": periodo.strftime("%m/%Y"),
                    "Registros": len(grupo)
                })
        
        # Exibir métricas
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="metric-container">
                <div class="metric-value">{len(responsaveis_novos)}</div>
                <div class="metric-label">Responsáveis</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-container">
                <div class="metric-value">{len(df_novo)}</div>
                <div class="metric-label">Registros Novos</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-container">
                <div class="metric-value">{len(combinacoes_novas)}</div>
                <div class="metric-label">Novos Períodos</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-container">
                <div class="metric-value">{len(combinacoes_existentes)}</div>
                <div class="metric-label">Períodos Atualizados</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Detalhes das operações
        if combinacoes_novas:
            with st.expander("➕ Novos Períodos que serão Adicionados"):
                df_novas = pd.DataFrame(combinacoes_novas)
                st.dataframe(df_novas, use_container_width=True, hide_index=True)
        
        if combinacoes_existentes:
            with st.expander("🔄 Períodos que serão Substituídos"):
                df_existentes = pd.DataFrame(combinacoes_existentes)
                st.dataframe(df_existentes, use_container_width=True, hide_index=True)
        
        return True
        
    except Exception as e:
        logger.error(f"Erro na análise pré-consolidação: {e}")
        st.error(f"❌ Erro na análise: {str(e)}")
        return False

def processar_consolidacao_com_lock(df_novo, nome_arquivo, token):
    """Consolidação com sistema de lock e feedback melhorado - v2.4.0"""
    session_id = gerar_id_sessao()
    
    status_container = st.empty()
    progress_container = st.empty()
    
    try:
        status_container.markdown("""
        <div class="custom-alert info">
            <h4>📄 Iniciando processo de consolidação v2.4.0...</h4>
        </div>
        """, unsafe_allow_html=True)
        
        sistema_ocupado, lock_data = verificar_lock_existente(token)
        if sistema_ocupado:
            status_container.markdown("""
            <div class="custom-alert error">
                <h4>🔒 Sistema ocupado! Outro usuário está fazendo consolidação.</h4>
            </div>
            """, unsafe_allow_html=True)
            return False
        
        status_container.markdown("""
        <div class="custom-alert info">
            <h4>🔒 Bloqueando sistema para consolidação...</h4>
        </div>
        """, unsafe_allow_html=True)
        progress_container.progress(10)
        
        lock_criado, session_lock = criar_lock(token, "Consolidação de planilha v2.4.0")
        
        if not lock_criado:
            status_container.markdown("""
            <div class="custom-alert error">
                <h4>❌ Não foi possível bloquear o sistema. Tente novamente.</h4>
            </div>
            """, unsafe_allow_html=True)
            return False
        
        status_container.markdown(f"""
        <div class="custom-alert success">
            <h4>✅ Sistema bloqueado com sucesso! (ID: {session_lock})</h4>
        </div>
        """, unsafe_allow_html=True)
        progress_container.progress(15)
        
        atualizar_status_lock(token, session_lock, "BAIXANDO_ARQUIVO", "Baixando arquivo consolidado")
        status_container.markdown("""
        <div class="custom-alert info">
            <h4>📥 Baixando arquivo consolidado existente...</h4>
        </div>
        """, unsafe_allow_html=True)
        progress_container.progress(25)
        
        df_consolidado, arquivo_existe = baixar_arquivo_consolidado(token)
        
        if arquivo_existe:
            status_container.markdown(f"""
            <div class="custom-alert info">
                <h4>📂 Arquivo consolidado carregado ({len(df_consolidado):,} registros)</h4>
            </div>
            """, unsafe_allow_html=True)
        else:
            status_container.markdown("""
            <div class="custom-alert info">
                <h4>📂 Criando novo arquivo consolidado</h4>
            </div>
            """, unsafe_allow_html=True)
        
        progress_container.progress(35)

        atualizar_status_lock(token, session_lock, "PREPARANDO_DADOS", "Validando e preparando dados")
        status_container.markdown("""
        <div class="custom-alert info">
            <h4>🔧 Preparando e validando dados...</h4>
        </div>
        """, unsafe_allow_html=True)
        
        df_novo = df_novo.copy()
        df_novo.columns = df_novo.columns.str.strip().str.upper()
        
        df_novo["DATA"] = pd.to_datetime(df_novo["DATA"], errors="coerce")
        linhas_invalidas = df_novo["DATA"].isna().sum()
        df_novo = df_novo.dropna(subset=["DATA"])

        if df_novo.empty:
            status_container.markdown("""
            <div class="custom-alert error">
                <h4>❌ Nenhum registro válido para consolidar</h4>
            </div>
            """, unsafe_allow_html=True)
            remover_lock(token, session_lock)
            return False

        if linhas_invalidas > 0:
            status_container.markdown(f"""
            <div class="custom-alert warning">
                <h4>🧹 {linhas_invalidas} linhas com datas inválidas foram removidas</h4>
            </div>
            """, unsafe_allow_html=True)

        progress_container.progress(45)

        status_container.markdown("""
        <div class="custom-alert info">
            <h4>📊 Realizando análise pré-consolidação...</h4>
        </div>
        """, unsafe_allow_html=True)
        analise_ok = analise_pre_consolidacao_v2(df_consolidado, df_novo)
        
        if not analise_ok:
            status_container.markdown("""
            <div class="custom-alert error">
                <h4>❌ Erro na análise pré-consolidação</h4>
            </div>
            """, unsafe_allow_html=True)
            remover_lock(token, session_lock)
            return False
        
        progress_container.progress(55)

        atualizar_status_lock(token, session_lock, "CONSOLIDANDO", f"Processando {len(df_novo)} registros por mês/ano")
        status_container.markdown("""
        <div class="custom-alert info">
            <h4>🔄 Processando consolidação (lógica por mês/ano v2.4.0)...</h4>
        </div>
        """, unsafe_allow_html=True)
        progress_container.progress(65)
        
        df_final, inseridos, substituidos, removidos, detalhes, novas_combinacoes, combinacoes_existentes = comparar_e_atualizar_registros_v2(
            df_consolidado, df_novo
        )
        
        progress_container.progress(75)

        status_container.markdown("""
        <div class="custom-alert info">
            <h4>🛡️ Executando verificação de segurança...</h4>
        </div>
        """, unsafe_allow_html=True)
        verificacao_ok, msg_verificacao = verificar_seguranca_consolidacao_v2(df_consolidado, df_novo, df_final)
        
        if not verificacao_ok:
            status_container.markdown(f"""
            <div class="custom-alert error">
                <h4>❌ ERRO DE SEGURANÇA: {msg_verificacao}</h4>
            </div>
            """, unsafe_allow_html=True)
            st.error("🛑 **Consolidação cancelada para proteger os dados!**")
            remover_lock(token, session_lock)
            return False
        else:
            status_container.markdown(f"""
            <div class="custom-alert success">
                <h4>✅ Verificação de segurança passou: {msg_verificacao}</h4>
            </div>
            """, unsafe_allow_html=True)

        df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"], na_position='last').reset_index(drop=True)
        progress_container.progress(80)
        
        if removidos > 0:
            atualizar_status_lock(token, session_lock, "CRIANDO_BACKUP", f"Backup de {removidos} registros substituídos")
            status_container.markdown("""
            <div class="custom-alert info">
                <h4>💾 Criando backup dos dados substituídos...</h4>
            </div>
            """, unsafe_allow_html=True)
        
        atualizar_status_lock(token, session_lock, "SALVANDO_ENVIADO", "Salvando cópia do arquivo enviado")
        status_container.markdown("""
        <div class="custom-alert info">
            <h4>💾 Salvando cópia do arquivo enviado...</h4>
        </div>
        """, unsafe_allow_html=True)
        salvar_arquivo_enviado(df_novo, nome_arquivo, token)
        
        progress_container.progress(85)
        
        atualizar_status_lock(token, session_lock, "UPLOAD_FINAL", "Salvando arquivo consolidado")
        status_container.markdown("""
        <div class="custom-alert info">
            <h4>📤 Salvando arquivo consolidado final...</h4>
        </div>
        """, unsafe_allow_html=True)
        
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False, sheet_name="Vendas CTs")
        buffer.seek(0)
        
        consolidado_nome = "Reports_Geral_Consolidado.xlsx"
        sucesso, status_code, resposta = upload_onedrive(consolidado_nome, buffer.read(), token, "consolidado")

        progress_container.progress(95)

        remover_lock(token, session_lock)
        progress_container.progress(100)
        
        if sucesso:
            status_container.empty()
            progress_container.empty()
            
            st.markdown("""
            <div class="custom-alert success">
                <h2>🎉 CONSOLIDAÇÃO REALIZADA COM SUCESSO!</h2>
                <p>🔓 Sistema liberado e disponível para outros usuários</p>
            </div>
            """, unsafe_allow_html=True)
            
            with st.expander("📍 Localização dos Arquivos", expanded=True):
                col1, col2 = st.columns(2)
                with col1:
                    st.info(f"📊 **Arquivo Consolidado:**\n`{PASTA_CONSOLIDADO}/Reports_Geral_Consolidado.xlsx`")
                with col2:
                    st.info(f"💾 **Backups e Envios:**\n`{PASTA_ENVIOS_BACKUPS}/`")
            
            st.markdown("### 📈 **Resultado da Consolidação**")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"""
                <div class="metric-container">
                    <div class="metric-value">{len(df_final):,}</div>
                    <div class="metric-label">📊 Total Final</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="metric-container">
                    <div class="metric-value">{inseridos}</div>
                    <div class="metric-label">➕ Inseridos</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                st.markdown(f"""
                <div class="metric-container">
                    <div class="metric-value">{substituidos}</div>
                    <div class="metric-label">🔄 Substituídos</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                st.markdown(f"""
                <div class="metric-container">
                    <div class="metric-value">{removidos}</div>
                    <div class="metric-label">🗑️ Removidos</div>
                </div>
                """, unsafe_allow_html=True)
            
            if novas_combinacoes > 0 or combinacoes_existentes > 0:
                st.markdown("### 📈 **Análise de Combinações (Responsável + Mês/Ano)**")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown(f"""
                    <div class="metric-container">
                        <div class="metric-value">{novas_combinacoes}</div>
                        <div class="metric-label">🆕 Novos Períodos</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div class="metric-container">
                        <div class="metric-value">{combinacoes_existentes}</div>
                        <div class="metric-label">🔄 Períodos Atualizados</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    total_processadas = novas_combinacoes + combinacoes_existentes
                    st.markdown(f"""
                    <div class="metric-container">
                        <div class="metric-value">{total_processadas}</div>
                        <div class="metric-label">📊 Total Processado</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                if novas_combinacoes > 0:
                    st.success(f"🎉 **{novas_combinacoes} novo(s) período(s) adicionado(s)** - Dados completamente novos!")
                if combinacoes_existentes > 0:
                    st.info(f"🔄 **{combinacoes_existentes} período(s) atualizado(s)** - Dados mensais completamente substituídos!")
            
            # Verificar se a coluna DATA_ULTIMO_ENVIO foi adicionada
            if 'DATA_ULTIMO_ENVIO' in df_final.columns:
                st.markdown("""
                <div class="custom-alert success">
                    <h4>📅 NOVO: Campo "Data do Último Envio" adicionado!</h4>
                    <p>A planilha consolidada agora inclui a data do último envio para cada responsável</p>
                </div>
                """, unsafe_allow_html=True)
            
            if detalhes:
                with st.expander("📋 Detalhes das Operações", expanded=removidos > 0):
                    df_detalhes = pd.DataFrame(detalhes)
                    
                    operacoes_inseridas = df_detalhes[df_detalhes['Operação'] == 'INSERIDO']
                    operacoes_substituidas = df_detalhes[df_detalhes['Operação'] == 'SUBSTITUÍDO']
                    operacoes_removidas = df_detalhes[df_detalhes['Operação'] == 'REMOVIDO']
                    
                    if not operacoes_inseridas.empty:
                        st.markdown("#### ➕ **Registros Inseridos (Novos)**")
                        st.dataframe(operacoes_inseridas, use_container_width=True, hide_index=True)
                    
                    if not operacoes_substituidas.empty:
                        st.markdown("#### 🔄 **Registros Substituídos**")
                        st.dataframe(operacoes_substituidas, use_container_width=True, hide_index=True)
                    
                    if not operacoes_removidas.empty:
                        st.markdown("#### 🗑️ **Registros Removidos**")
                        st.dataframe(operacoes_removidas, use_container_width=True, hide_index=True)
            
            if not df_final.empty:
                resumo_responsaveis = df_final.groupby("RESPONSÁVEL").agg({
                    "DATA": ["count", "min", "max"]
                }).round(0)
                
                resumo_responsaveis.columns = ["Total Registros", "Data Inicial", "Data Final"]
                resumo_responsaveis["Data Inicial"] = pd.to_datetime(resumo_responsaveis["Data Inicial"]).dt.strftime("%d/%m/%Y")
                resumo_responsaveis["Data Final"] = pd.to_datetime(resumo_responsaveis["Data Final"]).dt.strftime("%d/%m/%Y")
                
                # Adicionar informação sobre data do último envio se disponível
                if 'DATA_ULTIMO_ENVIO' in df_final.columns:
                    ultimo_envio = df_final.groupby("RESPONSÁVEL")["DATA_ULTIMO_ENVIO"].max()
                    ultimo_envio = ultimo_envio.dt.strftime("%d/%m/%Y %H:%M")
                    resumo_responsaveis["Último Envio"] = ultimo_envio
                
                with st.expander("👥 Resumo por Responsável"):
                    st.dataframe(resumo_responsaveis, use_container_width=True)
            
            return True
        else:
            status_container.markdown(f"""
            <div class="custom-alert error">
                <h4>❌ Erro no upload: Status {status_code}</h4>
            </div>
            """, unsafe_allow_html=True)
            if status_code != 500:
                st.code(resposta)
            return False
            
    except Exception as e:
        logger.error(f"Erro na consolidação: {e}")
        remover_lock(token, session_id, force=True)
        
        status_container.markdown(f"""
        <div class="custom-alert error">
            <h4>❌ Erro durante consolidação: {str(e)}</h4>
        </div>
        """, unsafe_allow_html=True)
        progress_container.empty()
        st.error("🔓 **Sistema liberado automaticamente após erro.**")
        return False

# ===========================
# INTERFACE STREAMLIT MELHORADA
# ===========================
def exibir_info_versao():
    """Exibe informações de versão e changelog com visual melhorado"""
    with st.sidebar:
        st.markdown("---")
        st.markdown("### ℹ️ Informações do Sistema")
        
        st.markdown(f"""
        <div class="status-card">
            <strong>Versão:</strong> {APP_VERSION}<br>
            <strong>Data:</strong> {VERSION_DATE}
        </div>
        """, unsafe_allow_html=True)
        
        if APP_VERSION == "2.4.0":
            st.markdown("""
            <div class="status-card success">
                <strong>🎨 VISUAL MELHORADO</strong><br>
                <strong>📅 CAMPO DATA ÚLTIMO ENVIO</strong>
            </div>
            """, unsafe_allow_html=True)
        
        with st.expander("📝 Configuração de Pastas"):
            st.markdown("**Arquivo Consolidado:**")
            st.code(PASTA_CONSOLIDADO, language=None)
            st.markdown("**Backups e Envios:**")
            st.code(PASTA_ENVIOS_BACKUPS, language=None)
        
        with st.expander("🆕 Novidades v2.4.0"):
            st.markdown("""
            **🎨 Visual Melhorado:**
            - Interface moderna e responsiva
            - CSS organizado e padronizado
            - Componentes visuais aprimorados
            - Animações e transições suaves
            
            **📅 Novo Campo:**
            - Data do último envio na planilha
            - Rastreamento por responsável
            - Atualização automática
            """)

def main():
    st.set_page_config(
        page_title=f"DSView BI - Upload Planilhas v{APP_VERSION}", 
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Aplicar estilos CSS melhorados
    aplicar_estilos_css()

    # Header principal melhorado
    st.markdown(f"""
    <div class="main-header fade-in">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <h1>📊 DSView BI — Upload de Planilhas</h1>
                <p style="margin: 0.5rem 0 0 0; opacity: 0.9;">Sistema de consolidação de relatórios</p>
            </div>
            <div class="version-badge">
                <strong>v{APP_VERSION}</strong><br>
                <small>{VERSION_DATE}</small>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if APP_VERSION == "2.4.0":
        st.markdown("""
        <div class="custom-alert success">
            <h4>🎨 VISUAL MELHORADO + 📅 CAMPO DATA ÚLTIMO ENVIO</h4>
            <p>Interface moderna e nova funcionalidade de rastreamento de envios!</p>
        </div>
        """, unsafe_allow_html=True)

    st.sidebar.markdown("### 📤 Upload de Planilhas")
    st.sidebar.markdown("Sistema de consolidação de relatórios")
    st.sidebar.divider()
    st.sidebar.markdown("**Status do Sistema:**")
    
    token = obter_token()
    if not token:
        st.sidebar.error("❌ Desconectado")
        st.error("❌ Não foi possível autenticar. Verifique as credenciais.")
        st.stop()
    else:
        st.sidebar.success("✅ Conectado")

    st.markdown("## 🔒 Status do Sistema")
    
    sistema_ocupado = exibir_status_sistema(token)
    
    if sistema_ocupado:
        st.markdown("---")
        st.info("🔄 Esta página será atualizada automaticamente a cada 15 segundos")
        time.sleep(15)
        st.rerun()

    st.divider()

    exibir_info_versao()

    st.markdown("## 📤 Upload de Planilha Excel")
    
    if sistema_ocupado:
        st.markdown("""
        <div class="custom-alert warning">
            <h4>⚠️ Upload desabilitado - Sistema em uso por outro usuário</h4>
            <p>💡 Aguarde a liberação do sistema ou tente novamente em alguns minutos</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🔄 Verificar Status Novamente"):
            st.rerun()
        
        return
    
    st.markdown("""
    <div class="custom-alert info">
        <h4>💡 Importante</h4>
        <p>A planilha deve conter uma coluna 'RESPONSÁVEL' com os nomes dos responsáveis!</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="custom-alert error">
        <h4>🔒 VALIDAÇÃO SUPER RIGOROSA ATIVADA</h4>
        <p>📋 QUALQUER problema de data (vazias, formato inválido, futuras, antigas) impedirá a consolidação!</p>
    </div>
    """, unsafe_allow_html=True)
    
    with st.expander("🎯 Novidades da v2.4.0 - VISUAL MELHORADO + DATA ÚLTIMO ENVIO", expanded=True):
        st.markdown("### 🎨 **Melhorias Visuais:**")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            **✨ Interface Moderna:**
            - CSS organizado e padronizado
            - Componentes visuais aprimorados
            - Layout responsivo
            - Animações suaves
            """)
            
        with col2:
            st.markdown("""
            **📊 Dashboard Melhorado:**
            - Métricas visuais aprimoradas
            - Cards de status modernos
            - Feedback visual durante processos
            - Tema consistente
            """)
        
        st.markdown("### 📅 **Nova Funcionalidade:**")
        st.success("🆕 **Campo 'Data do Último Envio'** - A planilha consolidada agora registra quando cada responsável teve seus dados atualizados pela última vez!")
        
        st.markdown("### 🔧 **Como Funciona:**")
        st.info("Quando você envia dados de um responsável, o sistema automaticamente registra a data e hora do envio na coluna 'DATA_ULTIMO_ENVIO'")
        st.info("Isso permite rastrear quando cada responsável teve seus dados atualizados pela última vez")
    
    st.divider()

    uploaded_file = st.file_uploader(
        "Escolha um arquivo Excel", 
        type=["xlsx", "xls"],
        help="Formatos aceitos: .xlsx, .xls | Certifique-se de que há uma coluna 'RESPONSÁVEL' na planilha"
    )

    df = None
    if uploaded_file:
        try:
            st.markdown(f"""
            <div class="custom-alert success">
                <h4>📁 Arquivo carregado: {uploaded_file.name}</h4>
            </div>
            """, unsafe_allow_html=True)
            
            file_extension = uploaded_file.name.split('.')[-1].lower()
            
            with st.spinner("📖 Lendo arquivo..."):
                if file_extension == 'xls':
                    xls = pd.ExcelFile(uploaded_file, engine='xlrd')
                else:
                    xls = pd.ExcelFile(uploaded_file)
                
                sheets = xls.sheet_names
                
                if len(sheets) > 1:
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
                        st.warning("⚠️ Recomendamos que a aba seja chamada 'Vendas CTs'")
                
                df = pd.read_excel(uploaded_file, sheet_name=sheet)
                df.columns = df.columns.str.strip().str.upper()
                
                st.success(f"✅ Dados carregados: {len(df)} linhas, {len(df.columns)} colunas")
                
                # Preview dos dados com visual melhorado
                with st.expander("👀 Preview dos Dados", expanded=True):
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.markdown(f"""
                        <div class="metric-container">
                            <div class="metric-value">{len(df)}</div>
                            <div class="metric-label">Linhas</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown(f"""
                        <div class="metric-container">
                            <div class="metric-value">{len(df.columns)}</div>
                            <div class="metric-label">Colunas</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        if "RESPONSÁVEL" in df.columns:
                            responsaveis_unicos = df["RESPONSÁVEL"].dropna().nunique()
                            st.markdown(f"""
                            <div class="metric-container">
                                <div class="metric-value">{responsaveis_unicos}</div>
                                <div class="metric-label">Responsáveis</div>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                            <div class="metric-container">
                                <div class="metric-value">❌</div>
                                <div class="metric-label">Responsáveis</div>
                            </div>
                            """, unsafe_allow_html=True)
                
        except Exception as e:
            st.error(f"❌ Erro ao ler arquivo: {str(e)}")
            st.stop()

    if df is not None:
        st.markdown("### 🔍 Validação dos Dados")
        
        with st.spinner("🔍 Validando dados..."):
            erros, avisos, problemas_datas = validar_dados_enviados(df)
        
        # Exibir resultados da validação
        if erros:
            st.markdown("""
            <div class="custom-alert error">
                <h4>❌ Problemas Encontrados</h4>
                <p>Corrija os problemas abaixo antes de prosseguir:</p>
            </div>
            """, unsafe_allow_html=True)
            
            for erro in erros:
                st.error(erro)
            
            if problemas_datas:
                exibir_problemas_datas(problemas_datas)
            
            botao_desabilitado = True
        else:
            st.markdown("""
            <div class="custom-alert success">
                <h4>✅ Validação Aprovada</h4>
                <p>Todos os dados estão válidos e prontos para consolidação!</p>
            </div>
            """, unsafe_allow_html=True)
            botao_desabilitado = False
        
        if avisos:
            for aviso in avisos:
                st.info(aviso)
        
        st.divider()
        
        # Botões de ação com visual melhorado
        if not erros:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                if botao_desabilitado:
                    st.button("❌ Consolidar Dados", type="primary", disabled=True, 
                             help="Corrija todos os problemas antes de prosseguir")
                    st.caption("🔒 Botão bloqueado - há problemas na planilha")
                else:
                    # Botão principal sem confirmação dupla
                    if st.button("✅ **Consolidar Dados**", type="primary", 
                                help="Inicia a consolidação por mês/ano imediatamente"):
                        
                        # Aviso importante antes de iniciar
                        st.markdown("""
                        <div class="custom-alert warning">
                            <h4>⏳ Consolidação iniciada! Aguarde o término do processo. NÃO feche esta página!</h4>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Iniciar consolidação diretamente
                        sucesso = processar_consolidacao_com_lock(df, uploaded_file.name, token)
                        
                        if sucesso:
                            st.balloons()
                            st.markdown("""
                            <div class="custom-alert success">
                                <h2>🎉 CONSOLIDAÇÃO FINALIZADA COM SUCESSO!</h2>
                                <p>💡 Você pode enviar uma nova planilha ou fechar esta página</p>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown("""
                            <div class="custom-alert error">
                                <h4>❌ Falha na consolidação. Tente novamente.</h4>
                            </div>
                            """, unsafe_allow_html=True)
            
            with col2:
                if st.button("🔄 Limpar Tela", type="secondary"):
                    st.rerun()
                    
        # Informações sobre o que a consolidação fará
        with st.expander("ℹ️ O que acontecerá durante a consolidação?", expanded=False):
            st.info("**📊 Análise dos dados enviados por mês/ano**")
            st.info("**🔄 Substituição de períodos mensais existentes** (mesmo responsável + mês/ano)")
            st.info("**➕ Adição de novos períodos** (combinações inexistentes)")
            st.info("**📅 Atualização da data do último envio** para responsáveis modificados")
            st.info("**💾 Criação de backups automáticos** dos dados substituídos")
            st.info("**🔒 Bloqueio temporário do sistema** durante o processo")
            st.info("**🛡️ Verificação de segurança** antes de salvar")
            st.info("**📈 Relatório completo** das operações realizadas")
            st.success("**🎯 NOVO:** Agora a consolidação é feita por **RESPONSÁVEL + MÊS/ANO** - elimina duplicatas!")
            st.success("**📅 NOVO:** Campo **DATA_ULTIMO_ENVIO** registra quando cada responsável foi atualizado!")

    # Footer melhorado
    st.markdown("---")
    st.markdown(f"""
    <div class="footer">
        <strong>DSView BI - Sistema de Consolidação de Relatórios v{APP_VERSION}</strong><br>
        ⚠️ Certifique-se de que sua planilha contenha:<br>
        • Uma aba chamada <strong>'Vendas CTs'</strong><br>
        • Uma coluna <strong>'DATA'</strong><br>
        • Uma coluna <strong>'RESPONSÁVEL'</strong><br>
        • Colunas: <strong>TMO - Duto, TMO - Freio, TMO - Sanit, TMO - Verniz, CX EVAP</strong><br>
        <br>
        🎨 <strong>v2.4.0:</strong> Visual melhorado + Campo data do último envio<br>
        <small>Última atualização: {VERSION_DATE}</small>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()

