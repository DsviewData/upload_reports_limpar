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
# CONFIGURAÇÕES DE VERSÃO - ATUALIZADO v2.2.1
# ===========================
APP_VERSION = "2.2.1"
VERSION_DATE = "2025-08-07"
CHANGELOG = {
    "2.2.1": {
        "date": "2025-08-07",
        "changes": [
            "🔒 Sistema de lock implementado - apenas 1 usuário pode enviar por vez",
            "⏱️ Timeout automático de 10 minutos para processos travados",
            "📊 Interface mostra status do sistema em tempo real",
            "🔄 Auto-refresh quando sistema está ocupado",
            "🆘 Botão de liberação forçada para processos órfãos",
            "📱 Session ID único para controle de concorrência",
            "🛡️ Proteção completa contra perda de dados em envios simultâneos"
        ]
    },
    "2.2.0": {
        "date": "2025-08-07",
        "changes": [
            "📁 Separação de pastas: arquivos enviados e backups agora ficam em pasta separada",
            "🗂️ Nova estrutura: consolidado em FontedeDados, envios/backups em PlanilhasEnviadas_Backups",
            "💾 Sistema de backup melhorado com localização dedicada",
            "📊 Interface atualizada mostrando localização dos arquivos",
            "🔧 Funções de upload especializadas por tipo de arquivo"
        ]
    },
    "2.1.0": {
        "date": "2025-08-07",
        "changes": [
            "🔍 Validação detalhada de datas com 6 tipos de problemas diferentes",
            "👥 Exibe responsável nas linhas com problemas",
            "📊 Categorização visual dos problemas (VAZIO, FORMATO, FUTURO, etc.)",
            "💡 Dicas específicas de correção para cada tipo de problema",
            "📈 Resumo visual por tipo de problema encontrado",
            "🏷️ Sistema de versionamento implementado"
        ]
    },
    "2.0.0": {
        "date": "2024-12-15",
        "changes": [
            "🔄 Nova lógica de consolidação completa",
            "💾 Sistema de backup automático melhorado",
            "📊 Métricas detalhadas de consolidação",
            "🎯 Interface aprimorada com resumos visuais"
        ]
    }
}

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
# CONFIGURAÇÃO DE PASTAS - v2.2.0
# ===========================
# PASTA PRINCIPAL - onde fica o arquivo consolidado
PASTA_CONSOLIDADO = "Documentos Compartilhados/LimparAuto/FontedeDados"

# NOVA PASTA - para arquivos enviados e backups
PASTA_ENVIOS_BACKUPS = "Documentos Compartilhados/PlanilhasEnviadas_Backups/LimparAuto"

# Manter compatibilidade (algumas funções ainda usam)
PASTA = PASTA_CONSOLIDADO

# ===========================
# CONFIGURAÇÃO DO SISTEMA DE LOCK - v2.2.1 NOVO
# ===========================
ARQUIVO_LOCK = "sistema_lock.json"
TIMEOUT_LOCK_MINUTOS = 10

# ===========================
# AUTENTICAÇÃO
# ===========================
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

# ===========================
# SISTEMA DE LOCK PARA CONTROLE DE CONCORRÊNCIA - v2.2.1 NOVO
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
            
            # Verificar se o lock não expirou
            timestamp_lock = datetime.fromisoformat(lock_data['timestamp'])
            agora = datetime.now()
            
            if agora - timestamp_lock > timedelta(minutes=TIMEOUT_LOCK_MINUTOS):
                # Lock expirado - remover automaticamente
                logger.info(f"Lock expirado removido automaticamente. Era de {timestamp_lock}")
                remover_lock(token, force=True)
                return False, None
            
            return True, lock_data
        
        elif response.status_code == 404:
            # Arquivo de lock não existe
            return False, None
        else:
            # Erro ao verificar - assumir que não há lock por segurança
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
            # Verificar se o lock pertence a esta sessão
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
            # Lock já não existe
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
        
        # Atualizar dados do lock
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
    """Exibe o status atual do sistema de lock"""
    lock_existe, lock_data = verificar_lock_existente(token)
    
    if lock_existe:
        timestamp_inicio = datetime.fromisoformat(lock_data['timestamp'])
        duracao = datetime.now() - timestamp_inicio
        
        # Determinar cor baseada na duração
        if duracao.total_seconds() < 60:  # Menos de 1 minuto
            cor = "🟡"  # Amarelo - normal
        elif duracao.total_seconds() < 300:  # Menos de 5 minutos  
            cor = "🟠"  # Laranja - demorado
        else:  # Mais de 5 minutos
            cor = "🔴"  # Vermelho - muito demorado
        
        # Calcular tempo restante até timeout
        tempo_limite = timestamp_inicio + timedelta(minutes=TIMEOUT_LOCK_MINUTOS)
        tempo_restante = tempo_limite - datetime.now()
        
        st.error(f"🔒 **Sistema ocupado** - Outro usuário está enviando dados")
        
        with st.expander("ℹ️ Detalhes do processo em andamento"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"**Operação:** {lock_data.get('operacao', 'N/A')}")
                st.info(f"**Status:** {lock_data.get('status', 'N/A')}")
                st.info(f"**Início:** {timestamp_inicio.strftime('%H:%M:%S')}")
                
            with col2:
                st.info(f"{cor} **Duração:** {int(duracao.total_seconds()//60)}min {int(duracao.total_seconds()%60)}s")
                if tempo_restante.total_seconds() > 0:
                    st.info(f"⏱️ **Timeout em:** {int(tempo_restante.total_seconds()//60)}min")
                else:
                    st.warning("⚠️ **Processo pode ter travado** (será liberado automaticamente)")
                
            # Detalhes adicionais se disponíveis
            if 'detalhes' in lock_data:
                st.info(f"**Detalhes:** {lock_data['detalhes']}")
                
            # Mostrar session ID para debug (apenas alguns caracteres)
            session_id_display = lock_data.get('session_id', 'N/A')[:8]
            st.caption(f"Session ID: {session_id_display}")
        
        # Botão de força para administradores (caso necessário)
        if tempo_restante.total_seconds() < 0:
            if st.button("🆘 Liberar Sistema (Forçar)", type="secondary"):
                if remover_lock(token, force=True):
                    st.success("✅ Sistema liberado com sucesso!")
                    st.rerun()
                else:
                    st.error("❌ Erro ao liberar sistema")
        
        return True  # Sistema ocupado
    else:
        # Sistema livre
        st.success("✅ **Sistema disponível** - Você pode enviar sua planilha")
        return False  # Sistema livre

# ===========================
# FUNÇÕES AUXILIARES
# ===========================

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

# ===========================
# UPLOAD E BACKUP - v2.2.0
# ===========================
def upload_onedrive(nome_arquivo, conteudo_arquivo, token, tipo_arquivo="consolidado"):
    """
    Faz upload de arquivo para OneDrive com pasta específica baseada no tipo
    
    tipo_arquivo: "consolidado", "enviado", "backup"
    """
    try:
        # Determinar pasta baseada no tipo
        if tipo_arquivo == "consolidado":
            pasta_base = PASTA_CONSOLIDADO
        elif tipo_arquivo in ["enviado", "backup"]:
            pasta_base = PASTA_ENVIOS_BACKUPS
        else:
            pasta_base = PASTA_CONSOLIDADO  # fallback
        
        # Garantir que a pasta existe
        pasta_arquivo = "/".join(nome_arquivo.split("/")[:-1]) if "/" in nome_arquivo else ""
        if pasta_arquivo:
            criar_pasta_se_nao_existir(f"{pasta_base}/{pasta_arquivo}", token)
        
        # Fazer backup se arquivo já existir (apenas para consolidado)
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
# VALIDAÇÃO MELHORADA DE DATAS
# ===========================
def validar_datas_detalhadamente(df):
    """
    🔍 NOVA VALIDAÇÃO DETALHADA DE DATAS - v2.1.0
    
    Detecta 6 tipos diferentes de problemas:
    1. VAZIO - Datas em branco ou nulas
    2. FORMATO - Formatos inválidos ou não conversíveis
    3. IMPOSSÍVEL - Datas logicamente impossíveis (31/02)
    4. FUTURO - Datas muito distantes no futuro
    5. ANTIGA - Datas muito antigas
    6. INCONSISTENTE - Outros problemas de consistência
    """
    from datetime import datetime
    import pandas as pd
    
    problemas = []
    
    logger.info(f"🔍 Iniciando validação detalhada de {len(df)} registros...")
    
    for idx, row in df.iterrows():
        linha_excel = idx + 2  # Excel começa em 1 + cabeçalho
        valor_original = row["DATA"]
        responsavel = row.get("RESPONSÁVEL", "N/A")
        
        problema_encontrado = None
        tipo_problema = None
        data_convertida = None
        
        # 1. VERIFICAR SE DATA ESTÁ VAZIA
        if pd.isna(valor_original) or str(valor_original).strip() == "":
            problema_encontrado = "Data vazia ou nula"
            tipo_problema = "VAZIO"
            
        else:
            try:
                # 2. TENTAR CONVERTER PARA DATETIME
                data_convertida = pd.to_datetime(valor_original, errors='raise')
                
                # 3. VERIFICAÇÕES DE LÓGICA DE NEGÓCIO
                hoje = datetime.now()
                ano_atual = hoje.year
                
                # Data muito no futuro (mais de 2 anos)
                if data_convertida > hoje + pd.Timedelta(days=730):
                    problema_encontrado = f"Data muito distante no futuro: {data_convertida.strftime('%d/%m/%Y')}"
                    tipo_problema = "FUTURO"
                
                # Data muito antiga (antes de 2020)
                elif data_convertida < pd.Timestamp('2020-01-01'):
                    problema_encontrado = f"Data muito antiga: {data_convertida.strftime('%d/%m/%Y')}"
                    tipo_problema = "ANTIGA"
                
                # Verificar datas impossíveis (ex: 31/02, 30/02, etc.)
                elif data_convertida.day > 31 or data_convertida.month > 12:
                    problema_encontrado = f"Data impossível: dia={data_convertida.day}, mês={data_convertida.month}"
                    tipo_problema = "IMPOSSÍVEL"
                    
                # Verificar se a data está num futuro muito próximo mas suspeito
                elif data_convertida > hoje + pd.Timedelta(days=90):
                    problema_encontrado = f"Data no futuro (verificar se está correta): {data_convertida.strftime('%d/%m/%Y')}"
                    tipo_problema = "FUTURO"
                    
            except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime, OverflowError) as e:
                # Data não pode ser convertida - problema de formato
                problema_encontrado = f"Formato inválido: '{str(valor_original)}'"
                tipo_problema = "FORMATO"
                logger.debug(f"Erro de conversão na linha {linha_excel}: {e}")
        
        # Se encontrou qualquer problema, adicionar aos detalhes
        if problema_encontrado:
            problemas.append({
                "Linha no Excel": linha_excel,
                "Data Inválida": str(valor_original)[:50],  # Limitar tamanho
                "Tipo Problema": tipo_problema,
                "Descrição": problema_encontrado,
                "Responsável": str(responsavel)[:30]  # Limitar tamanho
            })
    
    logger.info(f"✅ Validação concluída: {len(problemas)} problemas encontrados")
    return problemas

def exibir_relatorio_problemas_datas(problemas_datas):
    """
    📋 Exibe relatório visual detalhado dos problemas encontrados nas datas
    """
    if not problemas_datas:
        st.success("✅ **Todas as datas estão válidas e consistentes!**")
        return
    
    # Cabeçalho do relatório
    st.error(f"❗ **ATENÇÃO: {len(problemas_datas)} problemas encontrados nas datas**")
    
    # Converter para DataFrame para análise
    df_problemas = pd.DataFrame(problemas_datas)
    
    # 1. RESUMO POR TIPO DE PROBLEMA
    if "Tipo Problema" in df_problemas.columns:
        tipos_problema = df_problemas.groupby('Tipo Problema').size().sort_values(ascending=False)
        
        st.markdown("### 📊 **Resumo dos Problemas:**")
        
        # Criar colunas para mostrar os tipos
        cols = st.columns(min(len(tipos_problema), 4))
        
        emoji_map = {
            "VAZIO": "🔴",
            "FORMATO": "🟠", 
            "IMPOSSÍVEL": "🟣",
            "FUTURO": "🟡",
            "ANTIGA": "🟤",
            "INCONSISTENTE": "⚫"
        }
        
        for i, (tipo, qtd) in enumerate(tipos_problema.items()):
            emoji = emoji_map.get(tipo, "❌")
            col_idx = i % len(cols)
            
            with cols[col_idx]:
                st.metric(
                    label=f"{emoji} {tipo}",
                    value=f"{qtd} linha{'s' if qtd > 1 else ''}",
                    help=f"Problemas do tipo {tipo}"
                )
    
    st.divider()
    
    # 2. TABELA DETALHADA DOS PROBLEMAS
    st.markdown("### 📋 **Detalhes das Linhas com Problemas:**")
    
    # Preparar colunas para exibição
    colunas_exibir = ["Linha no Excel", "Responsável", "Data Inválida", "Descrição"]
    
    # Limitar número de linhas exibidas para não sobrecarregar
    max_linhas_exibir = 50
    
    if len(df_problemas) <= max_linhas_exibir:
        st.dataframe(
            df_problemas[colunas_exibir], 
            use_container_width=True, 
            hide_index=True,
            height=min(400, len(df_problemas) * 35)
        )
    else:
        st.dataframe(
            df_problemas.head(max_linhas_exibir)[colunas_exibir], 
            use_container_width=True, 
            hide_index=True,
            height=400
        )
        st.warning(f"⚠️ **Exibindo apenas as primeiras {max_linhas_exibir} linhas.** Total de problemas: {len(df_problemas)}")
    
    # 3. ANÁLISE POR RESPONSÁVEL
    if "Responsável" in df_problemas.columns:
        responsaveis_problemas = df_problemas.groupby('Responsável').size().sort_values(ascending=False)
        
        if len(responsaveis_problemas) > 1:
            st.markdown("### 👥 **Problemas por Responsável:**")
            
            col1, col2 = st.columns([2, 3])
            
            with col1:
                for responsavel, qtd in responsaveis_problemas.head(5).items():
                    st.write(f"• **{responsavel}**: {qtd} problema{'s' if qtd > 1 else ''}")
                    
                if len(responsaveis_problemas) > 5:
                    st.write(f"• ... e mais {len(responsaveis_problemas) - 5} responsáveis")
            
            with col2:
                # Gráfico simples de problemas por responsável
                st.bar_chart(responsaveis_problemas.head(10))
    
    st.divider()
    
    # 4. GUIA DE CORREÇÃO DETALHADA
    st.markdown("### 💡 **Guia de Correção:**")
    
    with st.expander("📖 Como corrigir cada tipo de problema", expanded=True):
        
        guia_correcao = {
            "VAZIO": {
                "emoji": "🔴",
                "problema": "Células de data em branco",
                "solucao": "Preencha com a data correta no formato DD/MM/AAAA",
                "exemplo": "Exemplo: 15/03/2024"
            },
            "FORMATO": {
                "emoji": "🟠", 
                "problema": "Formato de data inválido",
                "solucao": "Use apenas números no formato DD/MM/AAAA",
                "exemplo": "✅ 15/03/2024  ❌ '15 de março' ou 'março/2024'"
            },
            "IMPOSSÍVEL": {
                "emoji": "🟣",
                "problema": "Datas que não existem no calendário",
                "solucao": "Verifique dia e mês (ex: fevereiro não tem 30 dias)",
                "exemplo": "✅ 28/02/2024  ❌ 31/02/2024"
            },
            "FUTURO": {
                "emoji": "🟡",
                "problema": "Datas muito distantes no futuro",
                "solucao": "Verifique se o ano está correto",
                "exemplo": "Se for 2024, não use 2034"
            },
            "ANTIGA": {
                "emoji": "🟤",
                "problema": "Datas muito antigas (antes de 2020)",
                "solucao": "Confirme se o ano está correto",
                "exemplo": "Se for 2024, não use 2014"
            }
        }
        
        # Mostrar apenas as dicas para os tipos de problema encontrados
        tipos_encontrados = df_problemas['Tipo Problema'].unique()
        
        for tipo in tipos_encontrados:
            if tipo in guia_correcao:
                info = guia_correcao[tipo]
                
                st.markdown(f"""
                **{info['emoji']} {tipo}:**
                - **Problema:** {info['problema']}
                - **Solução:** {info['solucao']}
                - **{info['exemplo']}**
                """)
    
    # 5. BOTÃO DE DOWNLOAD DO RELATÓRIO
    with st.expander("💾 Baixar relatório de problemas"):
        if st.button("📥 Gerar Excel com problemas encontrados"):
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_problemas.to_excel(writer, index=False, sheet_name="Problemas_Datas")
            buffer.seek(0)
            
            st.download_button(
                label="⬇️ Baixar relatório de problemas",
                data=buffer.getvalue(),
                file_name=f"problemas_datas_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# ===========================
# VALIDAÇÃO COMPLETA MELHORADA
# ===========================
def validar_dados_enviados(df):
    """
    🔍 VALIDAÇÃO COMPLETA DOS DADOS ENVIADOS - v2.1.0
    
    Inclui validação melhorada de datas com detalhamento completo dos problemas
    """
    erros = []
    avisos = []
    linhas_invalidas_detalhes = []
    
    # Validar se DataFrame não está vazio
    if df.empty:
        erros.append("❌ A planilha está vazia")
        return erros, avisos, linhas_invalidas_detalhes
    
    # Validar se existe coluna RESPONSÁVEL
    if "RESPONSÁVEL" not in df.columns:
        erros.append("⚠️ A planilha deve conter uma coluna 'RESPONSÁVEL'")
        avisos.append("📋 Certifique-se de que sua planilha tenha uma coluna chamada 'RESPONSÁVEL'")
    else:
        # Validar se há responsáveis válidos
        responsaveis_validos = df["RESPONSÁVEL"].notna().sum()
        if responsaveis_validos == 0:
            erros.append("❌ Nenhum responsável válido encontrado na coluna 'RESPONSÁVEL'")
        else:
            # Mostrar responsáveis únicos encontrados
            responsaveis_unicos = df["RESPONSÁVEL"].dropna().unique()
            if len(responsaveis_unicos) > 0:
                avisos.append(f"👥 Responsáveis encontrados: {', '.join(responsaveis_unicos[:5])}")
                if len(responsaveis_unicos) > 5:
                    avisos.append(f"... e mais {len(responsaveis_unicos) - 5} responsáveis")
    
    # NOVA VALIDAÇÃO DETALHADA DE DATAS
    if "DATA" not in df.columns:
        erros.append("⚠️ A planilha deve conter uma coluna 'DATA'")
        avisos.append("📋 Lembre-se: o arquivo deve ter uma aba chamada 'Vendas CTs' com as colunas 'DATA' e 'RESPONSÁVEL'")
    else:
        # Executar validação detalhada de datas
        problemas_datas = validar_datas_detalhadamente(df)
        
        if problemas_datas:
            avisos.append(f"⚠️ {len(problemas_datas)} linhas com problemas de data serão ignoradas")
            
            # Converter para formato esperado pela interface
            linhas_invalidas_detalhes = problemas_datas
        else:
            avisos.append("✅ Todas as datas estão válidas e consistentes!")
    
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

# ===========================
# FUNÇÕES DE CONSOLIDAÇÃO - v2.2.0
# ===========================

def baixar_arquivo_consolidado(token):
    """Baixa o arquivo consolidado existente da pasta específica"""
    consolidado_nome = "Reports_Geral_Consolidado.xlsx"
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{PASTA_CONSOLIDADO}/{consolidado_nome}:/content"
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

def criar_backup_substituicoes(df_consolidado, detalhes_operacao, token):
    """Cria backup dos registros que foram substituídos na pasta de backups"""
    try:
        # Extrair apenas operações de remoção
        removidos = [d for d in detalhes_operacao if d["Operação"] == "REMOVIDO"]
        
        if not removidos:
            return
        
        # Identificar os registros que foram removidos
        registros_backup = []
        
        for item in removidos:
            responsavel = item["Responsável"]
            data_str = item["Data"]
            data = pd.to_datetime(data_str, format="%d/%m/%Y").date()
            
            mask = (
                (df_consolidado["DATA"].dt.date == data) &
                (df_consolidado["RESPONSÁVEL"].str.strip().str.upper() == str(responsavel).strip().upper())
            )
            
            registros_removidos = df_consolidado[mask]
            if not registros_removidos.empty:
                registros_backup.append(registros_removidos)
        
        if registros_backup:
            df_backup = pd.concat(registros_backup, ignore_index=True)
            
            # Adicionar metadados de backup
            df_backup["BACKUP_TIMESTAMP"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            df_backup["BACKUP_MOTIVO"] = "Substituição por novo envio"
            df_backup["APP_VERSION"] = APP_VERSION
            
            # Salvar backup NA NOVA PASTA
            timestamp = datetime.now().strftime('%d-%m-%Y_%Hh%M')
            nome_backup = f"Backups_Substituicoes/backup_substituicao_{timestamp}.xlsx"
            
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_backup.to_excel(writer, index=False, sheet_name="Registros Substituidos")
            buffer.seek(0)
            
            # USAR NOVA FUNÇÃO COM TIPO "backup"
            sucesso, _, _ = upload_onedrive(nome_backup, buffer.read(), token, "backup")
            if sucesso:
                st.info(f"💾 Backup dos dados substituídos criado: {PASTA_ENVIOS_BACKUPS}/{nome_backup}")
            else:
                st.warning("⚠️ Não foi possível criar backup dos dados substituídos")
                
    except Exception as e:
        st.warning(f"⚠️ Erro ao criar backup: {e}")
        logger.error(f"Erro no backup: {e}")

def comparar_e_atualizar_registros(df_consolidado, df_novo):
    """
    LÓGICA DE CONSOLIDAÇÃO COMPLETA:
    
    Para cada combinação RESPONSÁVEL + DATA no arquivo enviado:
    
    1. SE NÃO EXISTE na planilha consolidada:
       ➕ INSERE todos os novos registros
       
    2. SE JÁ EXISTE na planilha consolidada:
       🗑️ REMOVE todos os registros antigos dessa combinação
       ➕ INSERE todos os novos registros (SUBSTITUIÇÃO COMPLETA)
    
    Isso garante que:
    - Novos dados são sempre incluídos
    - Dados existentes são completamente atualizados
    - Não há conflitos ou dados parciais
    """
    registros_inseridos = 0
    registros_substituidos = 0
    registros_removidos = 0
    detalhes_operacao = []
    combinacoes_novas = 0
    combinacoes_existentes = 0
    
    if df_consolidado.empty:
        # Primeiro envio - todos os registros são novos
        df_final = df_novo.copy()
        registros_inseridos = len(df_novo)
        
        # Contar combinações únicas
        combinacoes_unicas = df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date]).size()
        combinacoes_novas = len(combinacoes_unicas)
        
        for _, row in df_novo.iterrows():
            detalhes_operacao.append({
                "Operação": "INSERIDO",
                "Responsável": row["RESPONSÁVEL"],
                "Data": row["DATA"].strftime("%d/%m/%Y"),
                "Motivo": "Primeira consolidação - arquivo vazio"
            })
        
        return df_final, registros_inseridos, registros_substituidos, registros_removidos, detalhes_operacao, combinacoes_novas, combinacoes_existentes
    
    # Garantir que as colunas existem no consolidado
    colunas = df_novo.columns.tolist()
    for col in colunas:
        if col not in df_consolidado.columns:
            df_consolidado[col] = None
    
    # Criar cópia para trabalhar
    df_final = df_consolidado.copy()
    
    # Agrupar registros novos por RESPONSÁVEL e DATA
    grupos_novos = df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date])
    
    for (responsavel, data_grupo), grupo_df in grupos_novos:
        if pd.isna(responsavel) or str(responsavel).strip() == '':
            continue
            
        # Buscar registros existentes para este responsável e data
        mask_existente = (
            (df_final["DATA"].dt.date == data_grupo) &
            (df_final["RESPONSÁVEL"].str.strip().str.upper() == str(responsavel).strip().upper())
        )
        
        registros_existentes = df_final[mask_existente]
        
        if not registros_existentes.empty:
            # ===== CENÁRIO 2: SUBSTITUIÇÃO COMPLETA =====
            num_removidos = len(registros_existentes)
            df_final = df_final[~mask_existente]
            registros_removidos += num_removidos
            combinacoes_existentes += 1
            
            # Adicionar detalhes da remoção
            detalhes_operacao.append({
                "Operação": "REMOVIDO",
                "Responsável": responsavel,
                "Data": data_grupo.strftime("%d/%m/%Y"),
                "Motivo": f"Substituição: {num_removidos} registro(s) antigo(s) removido(s)"
            })
            
            registros_substituidos += len(grupo_df)
            operacao_tipo = "SUBSTITUÍDO"
            motivo = f"Substituição completa: {len(grupo_df)} novo(s) registro(s)"
        else:
            # ===== CENÁRIO 1: INSERÇÃO DE NOVOS DADOS =====
            registros_inseridos += len(grupo_df)
            combinacoes_novas += 1
            operacao_tipo = "INSERIDO"
            motivo = f"Nova combinação: {len(grupo_df)} registro(s) inserido(s)"
        
        # Inserir novos registros (tanto para inserção quanto substituição)
        df_final = pd.concat([df_final, grupo_df], ignore_index=True)
        
        # Adicionar detalhes da operação
        detalhes_operacao.append({
            "Operação": operacao_tipo,
            "Responsável": responsavel,
            "Data": data_grupo.strftime("%d/%m/%Y"),
            "Motivo": motivo
        })
    
    return df_final, registros_inseridos, registros_substituidos, registros_removidos, detalhes_operacao, combinacoes_novas, combinacoes_existentes

def salvar_arquivo_enviado(df, nome_arquivo_original, token):
    """Salva o arquivo enviado com o nome original na nova pasta de enviados"""
    try:
        if not df.empty and "DATA" in df.columns:
            data_base = df["DATA"].min()
            nome_pasta = f"Relatorios_Enviados/{data_base.strftime('%Y-%m')}"
            timestamp = datetime.now().strftime('%d-%m-%Y_%Hh%M')
            
            # Usar nome original do arquivo com timestamp
            nome_sem_extensao = os.path.splitext(nome_arquivo_original)[0]
            nome_arquivo = f"{nome_pasta}/{nome_sem_extensao}_{timestamp}_v{APP_VERSION}.xlsx"
            
            # Salvar arquivo
            buffer_envio = BytesIO()
            with pd.ExcelWriter(buffer_envio, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name="Vendas CTs")
            buffer_envio.seek(0)
            
            # USAR NOVA FUNÇÃO COM TIPO "enviado"
            sucesso, _, _ = upload_onedrive(nome_arquivo, buffer_envio.read(), token, "enviado")
            if sucesso:
                st.info(f"💾 Arquivo salvo como: {PASTA_ENVIOS_BACKUPS}/{nome_arquivo}")
            else:
                st.warning("⚠️ Não foi possível salvar cópia do arquivo enviado")
                
    except Exception as e:
        st.warning(f"⚠️ Não foi possível salvar cópia do arquivo: {e}")
        logger.error(f"Erro ao salvar arquivo enviado: {e}")

# ===========================
# NOVA FUNÇÃO DE CONSOLIDAÇÃO COM LOCK - v2.2.1
# ===========================

def processar_consolidacao_com_lock(df_novo, nome_arquivo, token):
    """
    Versão protegida da consolidação com sistema de lock
    """
    session_id = gerar_id_sessao()
    
    try:
        # 1. Verificar se sistema está livre
        sistema_ocupado, lock_data = verificar_lock_existente(token)
        if sistema_ocupado:
            st.error("🔒 Sistema ocupado! Outro usuário está fazendo consolidação.")
            return False
        
        # 2. Criar lock
        st.info("🔒 Bloqueando sistema para consolidação...")
        lock_criado, session_lock = criar_lock(token, "Consolidação de planilha")
        
        if not lock_criado:
            st.error("❌ Não foi possível bloquear o sistema. Tente novamente.")
            return False
        
        st.success(f"✅ Sistema bloqueado com sucesso! (ID: {session_lock})")
        
        # 3. Atualizar status: baixando arquivo
        atualizar_status_lock(token, session_lock, "BAIXANDO_ARQUIVO", "Baixando arquivo consolidado")
        
        # 4. Baixar arquivo consolidado existente
        with st.spinner("📥 Baixando arquivo consolidado existente..."):
            df_consolidado, arquivo_existe = baixar_arquivo_consolidado(token)
        
        if arquivo_existe:
            st.info(f"📂 Arquivo consolidado carregado ({len(df_consolidado):,} registros)")
        else:
            st.info("📂 Criando novo arquivo consolidado")

        # 5. Atualizar status: preparando dados
        atualizar_status_lock(token, session_lock, "PREPARANDO_DADOS", "Validando e preparando dados")
        
        # 6. Preparar dados novos
        df_novo = df_novo.copy()
        df_novo.columns = df_novo.columns.str.strip().str.upper()
        
        # Converter datas e remover linhas inválidas
        df_novo["DATA"] = pd.to_datetime(df_novo["DATA"], errors="coerce")
        linhas_invalidas = df_novo["DATA"].isna().sum()
        df_novo = df_novo.dropna(subset=["DATA"])

        if df_novo.empty:
            st.error("❌ Nenhum registro válido para consolidar")
            remover_lock(token, session_lock)
            return False

        if linhas_invalidas > 0:
            st.info(f"🧹 {linhas_invalidas} linhas com datas inválidas foram removidas")

        # 7. Análise prévia dos dados
        responsaveis_no_envio = df_novo["RESPONSÁVEL"].dropna().unique()
        periodo_min = df_novo["DATA"].min().strftime("%d/%m/%Y")
        periodo_max = df_novo["DATA"].max().strftime("%d/%m/%Y")
        
        combinacoes_envio = df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date]).size()
        total_combinacoes = len(combinacoes_envio)
        
        st.info(f"👥 **Responsáveis:** {', '.join(responsaveis_no_envio)}")
        st.info(f"📅 **Período:** {periodo_min} até {periodo_max}")
        st.info(f"📊 **Combinações únicas (Responsável + Data):** {total_combinacoes}")
        
        # 8. Verificar se haverá substituições
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
                st.error("❌ Nenhum registro válido encontrado para processar")
                remover_lock(token, session_lock)
                return False
        else:
            st.success(f"✅ **{len(df_novo)} registro(s) serão CONSOLIDADOS** (primeira consolidação)")

        # 9. Atualizar status: processando consolidação
        atualizar_status_lock(token, session_lock, "CONSOLIDANDO", f"Processando {len(df_novo)} registros")
        
        # 10. Processar consolidação
        with st.spinner("🔄 Processando consolidação (nova lógica)..."):
            df_final, inseridos, substituidos, removidos, detalhes, novas_combinacoes, combinacoes_existentes = comparar_e_atualizar_registros(
                df_consolidado, df_novo
            )

        # 11. Ordenar por data e responsável
        df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"], na_position='last').reset_index(drop=True)
        
        # 12. Atualizar status: criando backups
        if removidos > 0:
            atualizar_status_lock(token, session_lock, "CRIANDO_BACKUP", f"Backup de {removidos} registros substituídos")
            criar_backup_substituicoes(df_consolidado, detalhes, token)
        
        # 13. Atualizar status: salvando arquivo enviado
        atualizar_status_lock(token, session_lock, "SALVANDO_ENVIADO", "Salvando cópia do arquivo enviado")
        salvar_arquivo_enviado(df_novo, nome_arquivo, token)
        
        # 14. Atualizar status: upload final
        atualizar_status_lock(token, session_lock, "UPLOAD_FINAL", "Salvando arquivo consolidado")
        
        # 15. Salvar arquivo consolidado
        with st.spinner("📤 Salvando arquivo consolidado..."):
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_final.to_excel(writer, index=False, sheet_name="Vendas CTs")
            buffer.seek(0)
            
            consolidado_nome = "Reports_Geral_Consolidado.xlsx"
            sucesso, status, resposta = upload_onedrive(consolidado_nome, buffer.read(), token, "consolidado")

        # 16. Remover lock SEMPRE (sucesso ou erro)
        remover_lock(token, session_lock)
        
        if sucesso:
            st.success("🔓 Sistema liberado!")
            st.success("✅ Consolidação realizada com sucesso!")
            
            # Mostrar localização dos arquivos
            with st.expander("📁 Localização dos Arquivos", expanded=True):
                col1, col2 = st.columns(2)
                with col1:
                    st.info(f"📊 **Arquivo Consolidado:**\n`{PASTA_CONSOLIDADO}/Reports_Geral_Consolidado.xlsx`")
                with col2:
                    st.info(f"💾 **Backups e Envios:**\n`{PASTA_ENVIOS_BACKUPS}/`")
            
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
            
            # Métricas de combinações
            if novas_combinacoes > 0 or combinacoes_existentes > 0:
                st.markdown("### 📈 Análise de Combinações (Responsável + Data)")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("🆕 Novas Combinações", novas_combinacoes, 
                             help="Combinações de Responsável + Data que não existiam antes")
                with col2:
                    st.metric("🔄 Combinações Atualizadas", combinacoes_existentes,
                             help="Combinações que já existiam e foram substituídas")
                with col3:
                    total_processadas = novas_combinacoes + combinacoes_existentes
                    st.metric("📊 Total Processado", total_processadas,
                             help="Total de combinações únicas processadas")
                
                if novas_combinacoes > 0:
                    st.success(f"🎉 **{novas_combinacoes} nova(s) combinação(ões) adicionada(s)** - Dados completamente novos!")
                if combinacoes_existentes > 0:
                    st.info(f"🔄 **{combinacoes_existentes} combinação(ões) atualizada(s)** - Dados existentes foram substituídos pelos novos!")
            
            # Detalhes das operações
            if detalhes:
                with st.expander("📋 Detalhes das Operações", expanded=removidos > 0):
                    df_detalhes = pd.DataFrame(detalhes)
                    
                    operacoes_inseridas = df_detalhes[df_detalhes['Operação'] == 'INSERIDO']
                    operacoes_substituidas = df_detalhes[df_detalhes['Operação'] == 'SUBSTITUÍDO']
                    operacoes_removidas = df_detalhes[df_detalhes['Operação'] == 'REMOVIDO']
                    
                    if not operacoes_inseridas.empty:
                        st.markdown("#### ➕ Registros Inseridos (Novos)")
                        st.dataframe(operacoes_inseridas, use_container_width=True, hide_index=True)
                    
                    if not operacoes_substituidas.empty:
                        st.markdown("#### 🔄 Registros Substituídos")
                        st.dataframe(operacoes_substituidas, use_container_width=True, hide_index=True)
                    
                    if not operacoes_removidas.empty:
                        st.markdown("#### 🗑️ Registros Removidos")
                        st.dataframe(operacoes_removidas, use_container_width=True, hide_index=True)
            
            # Resumo por responsável
            if not df_final.empty:
                resumo_responsaveis = df_final.groupby("RESPONSÁVEL").agg({
                    "DATA": ["count", "min", "max"]
                }).round(0)
                
                resumo_responsaveis.columns = ["Total Registros", "Data Inicial", "Data Final"]
                resumo_responsaveis["Data Inicial"] = pd.to_datetime(resumo_responsaveis["Data Inicial"]).dt.strftime("%d/%m/%Y")
                resumo_responsaveis["Data Final"] = pd.to_datetime(resumo_responsaveis["Data Final"]).dt.strftime("%d/%m/%Y")
                
                with st.expander("👥 Resumo por Responsável"):
                    st.dataframe(resumo_responsaveis, use_container_width=True)
            
            return True
        else:
            st.error(f"❌ Erro no upload: Status {status}")
            if status != 500:
                st.code(resposta)
            return False
            
    except Exception as e:
        # EM CASO DE ERRO, SEMPRE REMOVER O LOCK
        logger.error(f"Erro na consolidação: {e}")
        remover_lock(token, session_id, force=True)
        st.error(f"❌ Erro durante consolidação: {str(e)}")
        st.info("🔓 Sistema liberado automaticamente após erro.")
        return False

# ===========================
# INTERFACE STREAMLIT
# ===========================
def exibir_info_versao():
    """Exibe informações de versão e changelog"""
    with st.sidebar:
        st.markdown("---")
        st.markdown("### ℹ️ Informações do Sistema")
        st.info(f"**Versão:** {APP_VERSION}")
        st.info(f"**Data:** {VERSION_DATE}")
        
        # Mostrar configuração de pastas
        with st.expander("📁 Configuração de Pastas"):
            st.markdown("**Arquivo Consolidado:**")
            st.code(PASTA_CONSOLIDADO, language=None)
            st.markdown("**Backups e Envios:**")
            st.code(PASTA_ENVIOS_BACKUPS, language=None)
        
        with st.expander("📋 Changelog"):
            for version, info in CHANGELOG.items():
                st.markdown(f"#### v{version} ({info['date']})")
                for change in info['changes']:
                    st.markdown(f"- {change}")
                st.markdown("---")

def main():
    st.set_page_config(
        page_title=f"DSView BI - Upload Planilhas v{APP_VERSION}", 
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Header com logo e versão
    st.markdown(
        f'''
        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px;">
            <div style="display: flex; align-items: center; gap: 15px;">
                <h2 style="margin: 0; color: #2E8B57;">📊 DSView BI – Upload de Planilhas</h2>
            </div>
            <div style="text-align: right; color: #666; font-size: 0.9em;">
                <strong>v{APP_VERSION}</strong><br>
                <small>{VERSION_DATE}</small>
            </div>
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

    # ===========================
    # NOVO: VERIFICAÇÃO DE SISTEMA DE LOCK
    # ===========================
    st.markdown("## 🔒 Status do Sistema")
    
    # Verificar e exibir status do lock
    sistema_ocupado = exibir_status_sistema(token)
    
    # Auto-refresh a cada 15 segundos se sistema estiver ocupado
    if sistema_ocupado:
        st.markdown("---")
        st.info("🔄 Esta página será atualizada automaticamente a cada 15 segundos")
        time.sleep(15)
        st.rerun()

    st.divider()

    # Informações de versão na sidebar
    exibir_info_versao()

    st.markdown("## 📤 Upload de Planilha Excel")
    
    # Mostrar aviso se sistema estiver ocupado
    if sistema_ocupado:
        st.warning("⚠️ **Upload desabilitado** - Sistema em uso por outro usuário")
        st.info("💡 **Aguarde** a liberação do sistema ou tente novamente em alguns minutos")
        
        # Botão para atualizar status
        if st.button("🔄 Verificar Status Novamente"):
            st.rerun()
        
        # Não mostrar o resto da interface se sistema estiver ocupado
        return
    
    # Sistema livre - mostrar interface normal
    st.info("💡 **Importante**: A planilha deve conter uma coluna 'RESPONSÁVEL' com os nomes dos responsáveis!")
    
    # Nova estrutura de pastas
    with st.expander("📁 Nova Estrutura de Pastas - v2.2.0", expanded=False):
        st.markdown("### 🎯 Organização Melhorada dos Arquivos")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**📊 Arquivo Consolidado:**")
            st.code(f"{PASTA_CONSOLIDADO}/Reports_Geral_Consolidado.xlsx", language=None)
            st.caption("Arquivo principal com todos os dados consolidados")
            
        with col2:
            st.markdown("**💾 Backups e Envios:**")
            st.code(f"{PASTA_ENVIOS_BACKUPS}/", language=None)
            st.caption("Pasta separada para arquivos enviados e backups")
        
        st.success("✅ **Benefícios:** Melhor organização, backups separados e facilita a manutenção")
    
    st.divider()

    # Upload de arquivo (só aparece se sistema estiver livre)
    uploaded_file = st.file_uploader(
        "Escolha um arquivo Excel", 
        type=["xlsx", "xls"],
        help="Formatos aceitos: .xlsx, .xls | Certifique-se de que há uma coluna 'RESPONSÁVEL' na planilha"
    )

    # Processar arquivo carregado
    df = None
    if uploaded_file:
        try:
            st.success(f"📁 Arquivo carregado: **{uploaded_file.name}**")
            
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

        # NOVA VALIDAÇÃO MELHORADA
        st.subheader("🔍 Validações Detalhadas")
        erros, avisos, linhas_invalidas_detalhes = validar_dados_enviados(df)
        
        # Mostrar avisos
        for aviso in avisos:
            if aviso.startswith("✅"):
                st.success(aviso)
            else:
                st.warning(aviso)
        
        # Mostrar detalhes das linhas inválidas
        if linhas_invalidas_detalhes:
            exibir_relatorio_problemas_datas(linhas_invalidas_detalhes)
        
        # Mostrar erros
        if erros:
            for erro in erros:
                st.error(erro)

        # Botão de envio com verificação de lock
        col1, col2 = st.columns([1, 4])
        with col1:
            # Verificar novamente se sistema está livre antes de permitir envio
            sistema_ocupado_agora, _ = verificar_lock_existente(token)
            
            if sistema_ocupado_agora:
                st.error("🔒 Sistema foi bloqueado por outro usuário")
                if st.button("🔄 Atualizar Página"):
                    st.rerun()
            else:
                if st.button("📧 Consolidar Dados", type="primary", disabled=bool(erros)):
                    if erros:
                        st.error("❌ Corrija os erros acima antes de prosseguir")
                    else:
                        # Usar a nova função com lock
                        sucesso = processar_consolidacao_com_lock(df, uploaded_file.name, token)
                        if sucesso:
                            st.balloons()
                            
        with col2:
            if st.button("🔄 Limpar", type="secondary"):
                st.rerun()

    # Rodapé com informações
    st.divider()
    st.markdown(
        f"""
        <div style="text-align: center; color: #666; font-size: 0.8em;">
            <strong>DSView BI - Sistema de Consolidação de Relatórios v{APP_VERSION}</strong><br>
            ⚠️ Certifique-se de que sua planilha contenha:<br>
            • Uma aba chamada <strong>'Vendas CTs'</strong><br>
            • Uma coluna <strong>'DATA'</strong><br>
            • Uma coluna <strong>'RESPONSÁVEL'</strong><br>
            • Colunas: <strong>TMO - Duto, TMO - Freio, TMO - Sanit, TMO - Verniz, CX EVAP</strong><br>
            <br>
            📁 <strong>v2.2.1:</strong> Sistema de lock implementado - apenas 1 usuário por vez<br>
            <small>Última atualização: {VERSION_DATE}</small>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()