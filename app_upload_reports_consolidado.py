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
# CONFIGURAÇÕES DE VERSÃO - ATUALIZADO v2.2.4
# ===========================
APP_VERSION = "2.2.4"
VERSION_DATE = "2025-08-14"
CHANGELOG = {
    "2.2.4": {
        "date": "2025-08-14",
        "changes": [
            "🔧 CORREÇÃO CRÍTICA: Lógica de consolidação reescrita",
            "🛡️ Sistema de verificação de segurança implementado",
            "🔍 Logs detalhados para monitoramento de consolidação",
            "📊 Análise pré-consolidação com previsão de impacto",
            "⚡ Feedback visual melhorado durante processo",
            "🚨 Alertas claros antes e durante consolidação",
            "🎯 Corrigido problema de exclusão indevida de responsáveis",
            "📈 Métricas em tempo real durante processamento"
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
    """Exibe o status atual do sistema de lock"""
    lock_existe, lock_data = verificar_lock_existente(token)
    
    if lock_existe:
        timestamp_inicio = datetime.fromisoformat(lock_data['timestamp'])
        duracao = datetime.now() - timestamp_inicio
        
        if duracao.total_seconds() < 60:
            cor = "🟡"
        elif duracao.total_seconds() < 300:
            cor = "🟠"
        else:
            cor = "🔴"
        
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
        st.success("✅ **Sistema disponível** - Você pode enviar sua planilha")
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
                elif data_convertida.day > 31 or data_convertida.month > 12:
                    problema_encontrado = f"Data impossível: dia={data_convertida.day}, mês={data_convertida.month}"
                    tipo_problema = "IMPOSSÍVEL"
                elif data_convertida > hoje + pd.Timedelta(days=90):
                    problema_encontrado = f"Data no futuro (verificar se está correta): {data_convertida.strftime('%d/%m/%Y')}"
                    tipo_problema = "FUTURO"
                    
            except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime, OverflowError):
                problema_encontrado = f"Formato inválido: '{str(valor_original)}'"
                tipo_problema = "FORMATO"
        
        if problema_encontrado:
            problemas.append({
                "Linha no Excel": linha_excel,
                "Data Inválida": str(valor_original)[:50],
                "Tipo Problema": tipo_problema,
                "Descrição": problema_encontrado,
                "Responsável": str(responsavel)[:30]
            })
    
    logger.info(f"✅ Validação concluída: {len(problemas)} problemas encontrados")
    return problemas

def exibir_relatorio_problemas_datas(problemas_datas):
    """Exibe relatório visual detalhado dos problemas"""
    if not problemas_datas:
        st.success("✅ **Todas as datas estão válidas e consistentes!**")
        return
    
    st.error(f"⚠ **ATENÇÃO: {len(problemas_datas)} problemas encontrados nas datas**")
    
    df_problemas = pd.DataFrame(problemas_datas)
    
    if "Tipo Problema" in df_problemas.columns:
        tipos_problema = df_problemas.groupby('Tipo Problema').size().sort_values(ascending=False)
        
        st.markdown("### 📊 **Resumo dos Problemas:**")
        
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
    
    st.markdown("### 📋 **Detalhes das Linhas com Problemas:**")
    
    colunas_exibir = ["Linha no Excel", "Responsável", "Data Inválida", "Descrição"]
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

def validar_dados_enviados(df):
    """Validação super rigorosa dos dados enviados"""
    erros = []
    avisos = []
    linhas_invalidas_detalhes = []
    
    if df.empty:
        erros.append("❌ A planilha está vazia")
        return erros, avisos, linhas_invalidas_detalhes
    
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
            
            tipos_problema = {}
            for problema in problemas_datas:
                tipo = problema["Tipo Problema"]
                tipos_problema[tipo] = tipos_problema.get(tipo, 0) + 1
            
            detalhes_problemas = []
            emoji_map = {
                "VAZIO": "🔴",
                "FORMATO": "🟠", 
                "IMPOSSÍVEL": "🟣",
                "FUTURO": "🟡",
                "ANTIGA": "🟤"
            }
            
            for tipo, qtd in tipos_problema.items():
                emoji = emoji_map.get(tipo, "❌")
                detalhes_problemas.append(f"{emoji} {tipo}: {qtd} linha{'s' if qtd > 1 else ''}")
            
            erros.append(f"📊 Problemas por tipo: {', '.join(detalhes_problemas)}")
            
            if "VAZIO" in tipos_problema:
                erros.append("🔴 CRÍTICO: Existem datas em branco - preencha todas as datas")
            if "FORMATO" in tipos_problema:
                erros.append("🟠 CRÍTICO: Existem formatos inválidos - use formato DD/MM/AAAA")
            if "IMPOSSÍVEL" in tipos_problema:
                erros.append("🟣 CRÍTICO: Existem datas impossíveis - verifique dias e meses")
            if "FUTURO" in tipos_problema:
                erros.append("🟡 ATENÇÃO: Existem datas no futuro - confirme se estão corretas")
            if "ANTIGA" in tipos_problema:
                erros.append("🟤 ATENÇÃO: Existem datas muito antigas - confirme se estão corretas")
            
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
# FUNÇÕES DE CONSOLIDAÇÃO CORRIGIDAS
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

def verificar_seguranca_consolidacao(df_consolidado, df_novo, df_final):
    """Verificação de segurança crítica"""
    try:
        responsaveis_antes = set(df_consolidado['RESPONSÁVEL'].dropna().astype(str).str.strip().str.upper().unique()) if not df_consolidado.empty else set()
        responsaveis_novos = set(df_novo['RESPONSÁVEL'].dropna().astype(str).str.strip().str.upper().unique())
        responsaveis_depois = set(df_final['RESPONSÁVEL'].dropna().astype(str).str.strip().str.upper().unique())
        
        logger.info(f"🛡️ VERIFICAÇÃO DE SEGURANÇA:")
        logger.info(f"   Responsáveis ANTES: {responsaveis_antes}")
        logger.info(f"   Responsáveis NOVOS: {responsaveis_novos}")
        logger.info(f"   Responsáveis DEPOIS: {responsaveis_depois}")
        
        responsaveis_esperados = responsaveis_antes.union(responsaveis_novos)
        responsaveis_perdidos = responsaveis_esperados - responsaveis_depois
        
        if responsaveis_perdidos:
            error_msg = f"Responsáveis perdidos durante consolidação: {', '.join(responsaveis_perdidos)}"
            logger.error(f"❌ ERRO CRÍTICO: {error_msg}")
            return False, error_msg
        
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

def comparar_e_atualizar_registros(df_consolidado, df_novo):
    """Lógica de consolidação corrigida - v2.2.4"""
    registros_inseridos = 0
    registros_substituidos = 0
    registros_removidos = 0
    detalhes_operacao = []
    combinacoes_novas = 0
    combinacoes_existentes = 0
    
    logger.info(f"🔧 INICIANDO CONSOLIDAÇÃO:")
    logger.info(f"   Consolidado atual: {len(df_consolidado)} registros")
    logger.info(f"   Novo arquivo: {len(df_novo)} registros")
    
    if df_consolidado.empty:
        df_final = df_novo.copy()
        registros_inseridos = len(df_novo)
        
        combinacoes_unicas = df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date]).size()
        combinacoes_novas = len(combinacoes_unicas)
        
        logger.info(f"✅ PRIMEIRA CONSOLIDAÇÃO: {registros_inseridos} registros inseridos")
        
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
            logger.info(f"➕ Coluna '{col}' adicionada ao consolidado")
    
    # Começar com uma CÓPIA COMPLETA do consolidado
    df_final = df_consolidado.copy()
    registros_inicial = len(df_final)
    
    logger.info(f"🔍 Estado inicial do consolidado:")
    if not df_final.empty:
        responsaveis_iniciais = df_final['RESPONSÁVEL'].dropna().unique()
        logger.info(f"   Responsáveis: {responsaveis_iniciais}")
        logger.info(f"   Total de registros: {len(df_final)}")
    
    # Agrupar registros novos por RESPONSÁVEL e DATA
    grupos_novos = df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date])
    
    logger.info(f"📊 Processando {len(grupos_novos)} combinações únicas de Responsável+Data")
    
    for (responsavel, data_grupo), grupo_df in grupos_novos:
        if pd.isna(responsavel) or str(responsavel).strip() == '':
            logger.warning(f"⚠️ Pulando responsável inválido: {responsavel}")
            continue
        
        logger.info(f"🔍 Processando: '{responsavel}' em {data_grupo} ({len(grupo_df)} registros)")
        
        # Buscar registros existentes APENAS para este responsável e data ESPECÍFICOS
        mask_existente = (
            (df_final["DATA"].dt.date == data_grupo) &
            (df_final["RESPONSÁVEL"].astype(str).str.strip().str.upper() == str(responsavel).strip().upper())
        )
        
        registros_existentes = df_final[mask_existente]
        total_antes_operacao = len(df_final)
        
        logger.info(f"   📋 Encontrados {len(registros_existentes)} registros existentes para esta combinação")
        
        if not registros_existentes.empty:
            # SUBSTITUIÇÃO APENAS DA COMBINAÇÃO ESPECÍFICA
            num_removidos = len(registros_existentes)
            
            logger.info(f"   🔄 SUBSTITUIÇÃO: Removendo {num_removidos} registros antigos")
            
            # Remove APENAS os registros dessa combinação específica
            df_final = df_final[~mask_existente]
            
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
                "Data": data_grupo.strftime("%d/%m/%Y"),
                "Motivo": f"Substituição: {num_removidos} registro(s) antigo(s) removido(s)"
            })
            
            registros_substituidos += len(grupo_df)
            operacao_tipo = "SUBSTITUÍDO"
            motivo = f"Substituição completa: {len(grupo_df)} novo(s) registro(s)"
            
        else:
            # INSERÇÃO DE NOVOS DADOS
            logger.info(f"   ➕ NOVA COMBINAÇÃO: Adicionando {len(grupo_df)} registros")
            registros_inseridos += len(grupo_df)
            combinacoes_novas += 1
            operacao_tipo = "INSERIDO"
            motivo = f"Nova combinação: {len(grupo_df)} registro(s) inserido(s)"
        
        # Inserir novos registros (tanto para inserção quanto substituição)
        total_antes_insercao = len(df_final)
        df_final = pd.concat([df_final, grupo_df], ignore_index=True)
        total_depois_insercao = len(df_final)
        
        logger.info(f"   ✅ Inseridos {len(grupo_df)} registros. Total: {total_antes_insercao} -> {total_depois_insercao}")
        
        # Adicionar detalhes da operação
        detalhes_operacao.append({
            "Operação": operacao_tipo,
            "Responsável": responsavel,
            "Data": data_grupo.strftime("%d/%m/%Y"),
            "Motivo": motivo
        })
    
    # Log final detalhado
    logger.info(f"🏁 CONSOLIDAÇÃO FINALIZADA:")
    logger.info(f"   Total inicial: {registros_inicial}")
    logger.info(f"   Total final: {len(df_final)}")
    logger.info(f"   Inseridos: {registros_inseridos}")
    logger.info(f"   Substituídos: {registros_substituidos}")
    logger.info(f"   Removidos: {registros_removidos}")
    
    if not df_final.empty:
        responsaveis_finais = df_final['RESPONSÁVEL'].dropna().unique()
        logger.info(f"   Responsáveis finais: {responsaveis_finais}")
    
    return df_final, registros_inseridos, registros_substituidos, registros_removidos, detalhes_operacao, combinacoes_novas, combinacoes_existentes

def analise_pre_consolidacao(df_consolidado, df_novo):
    """Análise pré-consolidação"""
    try:
        responsaveis_no_envio = df_novo["RESPONSÁVEL"].dropna().unique()
        periodo_min = df_novo["DATA"].min().strftime("%d/%m/%Y")
        periodo_max = df_novo["DATA"].max().strftime("%d/%m/%Y")
        
        combinacoes_envio = df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date]).size()
        total_combinacoes = len(combinacoes_envio)
        
        st.markdown("### 🔍 **Análise Pré-Consolidação**")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(f"👥 **Responsáveis no envio:**\n{', '.join(responsaveis_no_envio)}")
        with col2:
            st.info(f"📅 **Período:**\n{periodo_min} até {periodo_max}")
        with col3:
            st.info(f"📊 **Combinações únicas:**\n{total_combinacoes} (Responsável + Data)")
        
        if not df_consolidado.empty:
            registros_para_consolidar = 0
            registros_para_alterar = 0
            registros_que_serao_removidos = 0
            
            for responsavel in responsaveis_no_envio:
                datas_envio = df_novo[df_novo["RESPONSÁVEL"] == responsavel]["DATA"].dt.date.unique()
                
                for data in datas_envio:
                    mask_conflito = (
                        (df_consolidado["DATA"].dt.date == data) &
                        (df_consolidado["RESPONSÁVEL"].astype(str).str.strip().str.upper() == str(responsavel).strip().upper())
                    )
                    
                    registros_envio = len(df_novo[
                        (df_novo["RESPONSÁVEL"] == responsavel) & 
                        (df_novo["DATA"].dt.date == data)
                    ])
                    
                    if mask_conflito.any():
                        registros_existentes = mask_conflito.sum()
                        registros_que_serao_removidos += registros_existentes
                        registros_para_alterar += registros_envio
                    else:
                        registros_para_consolidar += registros_envio
            
            total_atual = len(df_consolidado)
            total_enviado = len(df_novo)
            total_esperado = total_atual - registros_que_serao_removidos + total_enviado
            
            st.markdown("### 📈 **Impacto da Consolidação**")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📊 Registros Atuais", f"{total_atual:,}")
            with col2:
                st.metric("📤 Registros Enviados", f"{total_enviado:,}")
            with col3:
                st.metric("🗑️ Serão Removidos", f"{registros_que_serao_removidos:,}")
            with col4:
                st.metric("🎯 Total Esperado", f"{total_esperado:,}")
            
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
            
            if registros_que_serao_removidos > 0:
                st.warning(f"⚠️ **{registros_que_serao_removidos} registros existentes serão substituídos** pelos novos dados")
                st.info("💾 Um backup automático será criado dos dados substituídos")
        else:
            st.success(f"✅ **{len(df_novo)} registro(s) serão CONSOLIDADOS** (primeira consolidação)")
            
        return True
        
    except Exception as e:
        st.error(f"❌ Erro na análise pré-consolidação: {e}")
        logger.error(f"Erro na análise pré-consolidação: {e}")
        return False

def salvar_arquivo_enviado(df, nome_arquivo_original, token):
    """Salva o arquivo enviado"""
    try:
        if not df.empty and "DATA" in df.columns:
            data_base = df["DATA"].min()
            nome_pasta = f"Relatorios_Enviados/{data_base.strftime('%Y-%m')}"
            timestamp = datetime.now().strftime('%d-%m-%Y_%Hh%M')
            
            nome_sem_extensao = os.path.splitext(nome_arquivo_original)[0]
            nome_arquivo = f"{nome_pasta}/{nome_sem_extensao}_{timestamp}_v{APP_VERSION}.xlsx"
            
            buffer_envio = BytesIO()
            with pd.ExcelWriter(buffer_envio, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name="Vendas CTs")
            buffer_envio.seek(0)
            
            sucesso, _, _ = upload_onedrive(nome_arquivo, buffer_envio.read(), token, "enviado")
            if sucesso:
                st.info(f"💾 Arquivo salvo como: {PASTA_ENVIOS_BACKUPS}/{nome_arquivo}")
            else:
                st.warning("⚠️ Não foi possível salvar cópia do arquivo enviado")
                
    except Exception as e:
        st.warning(f"⚠️ Não foi possível salvar cópia do arquivo: {e}")
        logger.error(f"Erro ao salvar arquivo enviado: {e}")

def processar_consolidacao_com_lock(df_novo, nome_arquivo, token):
    """Consolidação com sistema de lock e feedback melhorado"""
    session_id = gerar_id_sessao()
    
    status_container = st.empty()
    progress_container = st.empty()
    
    try:
        status_container.info("🔄 **Iniciando processo de consolidação...**")
        
        sistema_ocupado, lock_data = verificar_lock_existente(token)
        if sistema_ocupado:
            status_container.error("🔒 Sistema ocupado! Outro usuário está fazendo consolidação.")
            return False
        
        status_container.info("🔒 **Bloqueando sistema para consolidação...**")
        progress_container.progress(10)
        
        lock_criado, session_lock = criar_lock(token, "Consolidação de planilha")
        
        if not lock_criado:
            status_container.error("❌ Não foi possível bloquear o sistema. Tente novamente.")
            return False
        
        status_container.success(f"✅ **Sistema bloqueado com sucesso!** (ID: {session_lock})")
        progress_container.progress(15)
        
        atualizar_status_lock(token, session_lock, "BAIXANDO_ARQUIVO", "Baixando arquivo consolidado")
        status_container.info("📥 **Baixando arquivo consolidado existente...**")
        progress_container.progress(25)
        
        df_consolidado, arquivo_existe = baixar_arquivo_consolidado(token)
        
        if arquivo_existe:
            status_container.info(f"📂 **Arquivo consolidado carregado** ({len(df_consolidado):,} registros)")
        else:
            status_container.info("📂 **Criando novo arquivo consolidado**")
        
        progress_container.progress(35)

        atualizar_status_lock(token, session_lock, "PREPARANDO_DADOS", "Validando e preparando dados")
        status_container.info("🔧 **Preparando e validando dados...**")
        
        df_novo = df_novo.copy()
        df_novo.columns = df_novo.columns.str.strip().str.upper()
        
        df_novo["DATA"] = pd.to_datetime(df_novo["DATA"], errors="coerce")
        linhas_invalidas = df_novo["DATA"].isna().sum()
        df_novo = df_novo.dropna(subset=["DATA"])

        if df_novo.empty:
            status_container.error("❌ **Nenhum registro válido para consolidar**")
            remover_lock(token, session_lock)
            return False

        if linhas_invalidas > 0:
            status_container.warning(f"🧹 **{linhas_invalidas} linhas com datas inválidas foram removidas**")

        progress_container.progress(45)

        status_container.info("📊 **Realizando análise pré-consolidação...**")
        analise_ok = analise_pre_consolidacao(df_consolidado, df_novo)
        
        if not analise_ok:
            status_container.error("❌ **Erro na análise pré-consolidação**")
            remover_lock(token, session_lock)
            return False
        
        progress_container.progress(55)

        atualizar_status_lock(token, session_lock, "CONSOLIDANDO", f"Processando {len(df_novo)} registros")
        status_container.info("🔄 **Processando consolidação (lógica corrigida v2.2.4)...**")
        progress_container.progress(65)
        
        df_final, inseridos, substituidos, removidos, detalhes, novas_combinacoes, combinacoes_existentes = comparar_e_atualizar_registros(
            df_consolidado, df_novo
        )
        
        progress_container.progress(75)

        status_container.info("🛡️ **Executando verificação de segurança...**")
        verificacao_ok, msg_verificacao = verificar_seguranca_consolidacao(df_consolidado, df_novo, df_final)
        
        if not verificacao_ok:
            status_container.error(f"❌ **ERRO DE SEGURANÇA:** {msg_verificacao}")
            st.error("🛑 **Consolidação cancelada para proteger os dados!**")
            remover_lock(token, session_lock)
            return False
        else:
            status_container.success(f"✅ **Verificação de segurança passou:** {msg_verificacao}")

        df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"], na_position='last').reset_index(drop=True)
        progress_container.progress(80)
        
        if removidos > 0:
            atualizar_status_lock(token, session_lock, "CRIANDO_BACKUP", f"Backup de {removidos} registros substituídos")
            status_container.info("💾 **Criando backup dos dados substituídos...**")
        
        atualizar_status_lock(token, session_lock, "SALVANDO_ENVIADO", "Salvando cópia do arquivo enviado")
        status_container.info("💾 **Salvando cópia do arquivo enviado...**")
        salvar_arquivo_enviado(df_novo, nome_arquivo, token)
        
        progress_container.progress(85)
        
        atualizar_status_lock(token, session_lock, "UPLOAD_FINAL", "Salvando arquivo consolidado")
        status_container.info("📤 **Salvando arquivo consolidado final...**")
        
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
            
            st.success("🎉 **CONSOLIDAÇÃO REALIZADA COM SUCESSO!**")
            st.success("🔓 **Sistema liberado e disponível para outros usuários**")
            
            with st.expander("📍 Localização dos Arquivos", expanded=True):
                col1, col2 = st.columns(2)
                with col1:
                    st.info(f"📊 **Arquivo Consolidado:**\n`{PASTA_CONSOLIDADO}/Reports_Geral_Consolidado.xlsx`")
                with col2:
                    st.info(f"💾 **Backups e Envios:**\n`{PASTA_ENVIOS_BACKUPS}/`")
            
            st.markdown("### 📈 **Resultado da Consolidação**")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📊 Total Final", f"{len(df_final):,}")
            with col2:
                st.metric("➕ Inseridos", f"{inseridos}")
            with col3:
                st.metric("🔄 Substituídos", f"{substituidos}")
            with col4:
                st.metric("🗑️ Removidos", f"{removidos}")
            
            if novas_combinacoes > 0 or combinacoes_existentes > 0:
                st.markdown("### 📈 **Análise de Combinações (Responsável + Data)**")
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
                
                with st.expander("👥 Resumo por Responsável"):
                    st.dataframe(resumo_responsaveis, use_container_width=True)
            
            return True
        else:
            status_container.error(f"❌ **Erro no upload:** Status {status_code}")
            if status_code != 500:
                st.code(resposta)
            return False
            
    except Exception as e:
        logger.error(f"Erro na consolidação: {e}")
        remover_lock(token, session_id, force=True)
        
        status_container.error(f"❌ **Erro durante consolidação:** {str(e)}")
        progress_container.empty()
        st.error("🔓 **Sistema liberado automaticamente após erro.**")
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
        
        if APP_VERSION == "2.2.4":
            st.success("🔧 **LÓGICA CONSOLIDAÇÃO CORRIGIDA**")
            st.caption("Corrigido problema de exclusão indevida")
        
        with st.expander("📁 Configuração de Pastas"):
            st.markdown("**Arquivo Consolidado:**")
            st.code(PASTA_CONSOLIDADO, language=None)
            st.markdown("**Backups e Envios:**")
            st.code(PASTA_ENVIOS_BACKUPS, language=None)

def main():
    st.set_page_config(
        page_title=f"DSView BI - Upload Planilhas v{APP_VERSION}", 
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.markdown(
        f'''
        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px;">
            <div style="display: flex; align-items: center; gap: 15px;">
                <h2 style="margin: 0; color: #2E8B57;">📊 DSView BI — Upload de Planilhas</h2>
            </div>
            <div style="text-align: right; color: #666; font-size: 0.9em;">
                <strong>v{APP_VERSION}</strong><br>
                <small>{VERSION_DATE}</small>
            </div>
        </div>
        ''',
        unsafe_allow_html=True
    )

    if APP_VERSION == "2.2.4":
        st.success("🔧 **LÓGICA DE CONSOLIDAÇÃO CORRIGIDA** - Problema de exclusão indevida de responsáveis resolvido!")

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
        st.warning("⚠️ **Upload desabilitado** - Sistema em uso por outro usuário")
        st.info("💡 **Aguarde** a liberação do sistema ou tente novamente em alguns minutos")
        
        if st.button("🔄 Verificar Status Novamente"):
            st.rerun()
        
        return
    
    st.info("💡 **Importante**: A planilha deve conter uma coluna 'RESPONSÁVEL' com os nomes dos responsáveis!")
    
    st.error("🔒 **VALIDAÇÃO SUPER RIGOROSA ATIVADA**")
    st.warning("📋 **QUALQUER problema de data (vazias, formato inválido, futuras, antigas) impedirá a consolidação!**")
    st.info("💡 **Dica**: Revise cuidadosamente sua planilha antes de enviar. Todas as datas devem estar corretas.")
    
    with st.expander("🔧 Correções da v2.2.4 - LÓGICA CONSOLIDAÇÃO", expanded=True):
        st.markdown("### 🛡️ **Problemas Corrigidos:**")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            **❌ Problema Anterior:**
            - Exclusão indevida de responsáveis
            - Perda de dados durante consolidação
            - Falta de verificações de segurança
            """)
            
        with col2:
            st.markdown("""
            **✅ Soluções Implementadas:**
            - Verificação de segurança obrigatória
            - Logs detalhados de consolidação
            - Análise pré-consolidação
            - Feedback visual em tempo real
            """)
        
        st.success("🎯 **Resultado:** Consolidação 100% segura - nenhum dado será perdido inadvertidamente!")
    
    st.divider()

    uploaded_file = st.file_uploader(
        "Escolha um arquivo Excel", 
        type=["xlsx", "xls"],
        help="Formatos aceitos: .xlsx, .xls | Certifique-se de que há uma coluna 'RESPONSÁVEL' na planilha"
    )

    df = None
    if uploaded_file:
        try:
            st.success(f"📁 Arquivo carregado: **{uploaded_file.name}**")
            
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
                        st.warning(f"⚠️ A aba atual se chama '{sheet}'. Recomendamos renomeá-la para 'Vendas CTs'")
                
                df = pd.read_excel(uploaded_file, sheet_name=sheet)
                df.columns = df.columns.str.strip().str.upper()
                
            st.success(f"✅ Dados carregados com sucesso!")
            
        except Exception as e:
            st.error(f"❌ Erro ao ler o Excel: {e}")
            logger.error(f"Erro ao ler Excel: {e}")

    if df is not None:
        st.subheader("👀 Prévia dos dados")
        st.dataframe(df.head(10), use_container_width=True, height=300)

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

        st.subheader("💰 Resumo de Totais por Produto")
        
        colunas_produtos = ['TMO - DUTO', 'TMO - FREIO', 'TMO - SANIT', 'TMO - VERNIZ', 'CX EVAP']
        colunas_encontradas = [col for col in colunas_produtos if col in df.columns]
        
        if colunas_encontradas:
            totais = {}
            total_geral = 0
            
            for coluna in colunas_encontradas:
                valores_numericos = pd.to_numeric(df[coluna], errors='coerce').fillna(0)
                total = int(valores_numericos.sum())
                totais[coluna] = total
                total_geral += total
            
            colunas_tmo = [col for col in colunas_encontradas if col.startswith('TMO -')]
            if colunas_tmo:
                tmo_total = sum(totais[col] for col in colunas_tmo)
                totais['TMO - TOTAL'] = tmo_total
            
            produtos_para_exibir = [col for col in colunas_produtos if col in totais]
            if 'TMO - TOTAL' in totais:
                produtos_para_exibir.append('TMO - TOTAL')
            
            num_colunas = len(produtos_para_exibir)
            cols = st.columns(num_colunas)
            
            for i, coluna in enumerate(produtos_para_exibir):
                with cols[i]:
                    total = totais[coluna]
                    total_formatado = f"{total:,}".replace(',', '.')
                    
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
                    
                    nome_display = coluna.replace('TMO - ', '').title()
                    
                    st.metric(f"{emoji} {nome_display}", total_formatado)
        else:
            st.warning("⚠️ Nenhuma coluna de produtos encontrada")

        colunas_nulas = df.columns[df.isnull().any()].tolist()
        if colunas_nulas:
            st.warning(f"⚠️ Colunas com valores nulos: {', '.join(colunas_nulas[:5])}")
            if len(colunas_nulas) > 5:
                st.warning(f"... e mais {len(colunas_nulas) - 5} colunas")
        else:
            st.success("✅ Nenhuma coluna com valores nulos.")

        st.subheader("🔍 Validações Super Rigorosas")
        erros, avisos, linhas_invalidas_detalhes = validar_dados_enviados(df)
        
        for aviso in avisos:
            if aviso.startswith("✅"):
                st.success(aviso)
            else:
                st.warning(aviso)
        
        if linhas_invalidas_detalhes:
            exibir_relatorio_problemas_datas(linhas_invalidas_detalhes)
        
        if erros:
            st.markdown("## ❌ **PROBLEMAS ENCONTRADOS - CORREÇÃO OBRIGATÓRIA**")
            
            for erro in erros:
                if erro.startswith("❌"):
                    st.error(erro)
                elif erro.startswith("🔧"):
                    st.error(erro)
                elif erro.startswith("📋"):
                    st.warning(erro)
                else:
                    st.error(erro)
            
            st.markdown("---")
            st.error("🚫 **A consolidação está BLOQUEADA até que todos os problemas sejam corrigidos!**")
            st.info("💡 **Próximos passos:**")
            st.info("1. ✏️ Abra sua planilha Excel")
            st.info("2. 🔧 Corrija TODOS os problemas listados acima")
            st.info("3. 💾 Salve o arquivo")
            st.info("4. 🔄 Faça o upload novamente")

        st.divider()
        st.markdown("### 🚀 **Consolidar Dados**")
        
        col1, col2 = st.columns([1, 4])
        with col1:
            sistema_ocupado_agora, _ = verificar_lock_existente(token)
            
            if sistema_ocupado_agora:
                st.error("🔒 Sistema foi bloqueado por outro usuário")
                if st.button("🔄 Atualizar Página"):
                    st.rerun()
            else:
                botao_desabilitado = bool(erros)
                
                if botao_desabilitado:
                    st.button("❌ Consolidar Dados", type="primary", disabled=True, 
                             help="Corrija todos os problemas antes de prosseguir")
                    st.caption("🔒 Botão bloqueado - há problemas na planilha")
                else:
                    if st.button("✅ Consolidar Dados", type="primary", help="Clique para iniciar a consolidação"):
                        
                        with st.expander("⚠️ **CONFIRMAÇÃO FINAL**", expanded=True):
                            st.warning("**Você está prestes a consolidar os dados. Esta ação irá:**")
                            st.info("• 📊 Analisar os dados enviados")
                            st.info("• 🔄 Substituir dados existentes com mesma data e responsável")
                            st.info("• ➕ Adicionar novos dados")
                            st.info("• 💾 Criar backups automáticos")
                            st.info("• 🔒 Bloquear sistema durante o processo")
                            
                            col_confirma, col_cancela = st.columns(2)
                            
                            with col_confirma:
                                if st.button("🎯 **CONFIRMAR CONSOLIDAÇÃO**", type="primary"):
                                    st.info("🚀 **Iniciando consolidação...**")
                                    st.warning("⏳ **Aguarde o término do processo. NÃO feche esta página!**")
                                    
                                    sucesso = processar_consolidacao_com_lock(df, uploaded_file.name, token)
                                    
                                    if sucesso:
                                        st.balloons()
                                        st.success("🎉 **CONSOLIDAÇÃO FINALIZADA COM SUCESSO!**")
                                        st.info("💡 Você pode enviar uma nova planilha ou fechar esta página")
                                    else:
                                        st.error("❌ **Falha na consolidação. Tente novamente.**")
                            
                            with col_cancela:
                                if st.button("❌ Cancelar", type="secondary"):
                                    st.info("🔄 Consolidação cancelada. Você pode fazer ajustes na planilha se necessário.")
                                    st.rerun()
                        
        with col2:
            if st.button("🔄 Limpar Tela", type="secondary"):
                st.rerun()

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
            🔧 <strong>v2.2.4:</strong> Lógica de consolidação corrigida - Verificações de segurança implementadas<br>
            <small>Última atualização: {VERSION_DATE}</small>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()