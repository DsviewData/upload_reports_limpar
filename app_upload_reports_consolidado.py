import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
from msal import ConfidentialClientApplication
import unicodedata
import logging
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

# === FUNÇÕES DE CONSOLIDAÇÃO MELHORADAS ===
def validar_dados_enviados(df):
    """Valida os dados enviados pelo usuário"""
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
    
    # Validar se existe coluna DATA
    if "DATA" not in df.columns:
        erros.append("⚠️ A planilha deve conter uma coluna 'DATA'")
        avisos.append("📋 Lembre-se: o arquivo deve ter uma aba chamada 'Vendas CTs' com as colunas 'DATA' e 'RESPONSÁVEL'")
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

def criar_backup_substituicoes(df_consolidado, detalhes_operacao, token):
    """Cria backup dos registros que foram substituídos"""
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
            
            # Salvar backup
            timestamp = datetime.now().strftime('%d-%m-%Y_%Hh%M')
            nome_backup = f"Backups_Substituicoes/backup_substituicao_{timestamp}.xlsx"
            
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_backup.to_excel(writer, index=False, sheet_name="Registros Substituidos")
            buffer.seek(0)
            
            sucesso, _, _ = upload_onedrive(nome_backup, buffer.read(), token)
            if sucesso:
                st.info(f"💾 Backup dos dados substituídos criado: {nome_backup}")
            else:
                st.warning("⚠️ Não foi possível criar backup dos dados substituídos")
                
    except Exception as e:
        st.warning(f"⚠️ Erro ao criar backup: {e}")
        logger.error(f"Erro no backup: {e}")

def comparar_e_atualizar_registros(df_consolidado, df_novo):
    """
    NOVA LÓGICA DE CONSOLIDAÇÃO COMPLETA:
    
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

def processar_consolidacao(df_novo, nome_arquivo, token):
    """
    Versão melhorada do processamento de consolidação
    """
    
    # 1. Baixar arquivo consolidado existente
    with st.spinner("📥 Baixando arquivo consolidado existente..."):
        df_consolidado, arquivo_existe = baixar_arquivo_consolidado(token)
    
    if arquivo_existe:
        st.info(f"📂 Arquivo consolidado carregado ({len(df_consolidado):,} registros)")
    else:
        st.info("📂 Criando novo arquivo consolidado")

    # 2. Preparar dados novos
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

    # Análise prévia dos dados
    responsaveis_no_envio = df_novo["RESPONSÁVEL"].dropna().unique()
    periodo_min = df_novo["DATA"].min().strftime("%d/%m/%Y")
    periodo_max = df_novo["DATA"].max().strftime("%d/%m/%Y")
    
    # Contar combinações RESPONSÁVEL + DATA no envio
    combinacoes_envio = df_novo.groupby(['RESPONSÁVEL', df_novo['DATA'].dt.date]).size()
    total_combinacoes = len(combinacoes_envio)
    
    st.info(f"👥 **Responsáveis:** {', '.join(responsaveis_no_envio)}")
    st.info(f"📅 **Período:** {periodo_min} até {periodo_max}")
    st.info(f"📊 **Combinações únicas (Responsável + Data):** {total_combinacoes}")
    
    # Verificar se haverá substituições
    if arquivo_existe and not df_consolidado.empty:
        df_consolidado["DATA"] = pd.to_datetime(df_consolidado["DATA"], errors="coerce")
        df_consolidado = df_consolidado.dropna(subset=["DATA"])
        
        # Verificar conflitos
        conflitos = []
        for responsavel in responsaveis_no_envio:
            datas_envio = df_novo[df_novo["RESPONSÁVEL"] == responsavel]["DATA"].dt.date.unique()
            
            for data in datas_envio:
                mask_conflito = (
                    (df_consolidado["DATA"].dt.date == data) &
                    (df_consolidado["RESPONSÁVEL"].str.strip().str.upper() == str(responsavel).strip().upper())
                )
                
                if mask_conflito.any():
                    num_existentes = mask_conflito.sum()
                    num_novos = len(df_novo[
                        (df_novo["RESPONSÁVEL"] == responsavel) & 
                        (df_novo["DATA"].dt.date == data)
                    ])
                    
                    conflitos.append({
                        "Responsável": responsavel,
                        "Data": data.strftime("%d/%m/%Y"),
                        "Existentes": num_existentes,
                        "Novos": num_novos
                    })
        
        # Mostrar conflitos se existirem
        if conflitos:
            st.warning("⚠️ **ATENÇÃO: Os seguintes dados serão SUBSTITUÍDOS:**")
            
            df_conflitos = pd.DataFrame(conflitos)
            st.dataframe(df_conflitos, use_container_width=True, hide_index=True)
            
            total_substituicoes = sum(c["Existentes"] for c in conflitos)
            st.warning(f"📝 **{total_substituicoes} registro(s) existente(s) serão removidos e substituídos**")
            
            # Opção de confirmação
            confirmacao = st.checkbox(
                "✅ Confirmo que desejo substituir os dados existentes pelos novos dados",
                help="Esta ação não pode ser desfeita. Os dados antigos serão movidos para backup."
            )
            
            if not confirmacao:
                st.info("⏸️ Marque a confirmação acima para prosseguir com a consolidação")
                return False

    # 3. Processar consolidação com nova lógica
    with st.spinner("🔄 Processando consolidação (nova lógica)..."):
        df_final, inseridos, substituidos, removidos, detalhes, novas_combinacoes, combinacoes_existentes = comparar_e_atualizar_registros(
            df_consolidado, df_novo
        )

    # 4. Ordenar por data e responsável
    df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"], na_position='last').reset_index(drop=True)
    
    # 5. Criar backup dos dados removidos se houve substituições
    if removidos > 0:
        criar_backup_substituicoes(df_consolidado, detalhes, token)
    
    # 6. Salvar arquivo enviado com nome original
    salvar_arquivo_enviado(df_novo, nome_arquivo, token)
    
    # 7. Salvar arquivo consolidado
    with st.spinner("📤 Salvando arquivo consolidado..."):
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df_final.to_excel(writer, index=False, sheet_name="Vendas CTs")
        buffer.seek(0)
        
        consolidado_nome = "Reports_Geral_Consolidado.xlsx"
        sucesso, status, resposta = upload_onedrive(consolidado_nome, buffer.read(), token)

    if sucesso:
        st.success("✅ Consolidação realizada com sucesso!")
        
        # Métricas de resultado melhoradas
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
            
            # Explicação visual
            if novas_combinacoes > 0:
                st.success(f"🎉 **{novas_combinacoes} nova(s) combinação(ões) adicionada(s)** - Dados completamente novos!")
            if combinacoes_existentes > 0:
                st.info(f"🔄 **{combinacoes_existentes} combinação(ões) atualizada(s)** - Dados existentes foram substituídos pelos novos!")
        
        # Detalhes das operações
        if detalhes:
            with st.expander("📋 Detalhes das Operações", expanded=removidos > 0):
                df_detalhes = pd.DataFrame(detalhes)
                
                # Separar por tipo de operação para melhor visualização
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

def salvar_arquivo_enviado(df, nome_arquivo_original, token):
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
    st.info("💡 **Importante**: A planilha deve conter uma coluna 'RESPONSÁVEL' com os nomes dos responsáveis!")
    st.divider()

    # Upload de arquivo
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

        # Validações antes do envio
        st.subheader("🔍 Validações")
        erros, avisos, linhas_invalidas_detalhes = validar_dados_enviados(df)
        
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
            • Uma coluna <strong>'RESPONSÁVEL'</strong><br>
            • Colunas: <strong>TMO - Duto, TMO - Freio, TMO - Sanit, TMO - Verniz, CX EVAP</strong>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()