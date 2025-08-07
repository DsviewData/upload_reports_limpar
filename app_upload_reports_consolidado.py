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
    
    # Colunas esperadas do sistema
    COLUNAS_ESPERADAS = [
        'GRUPO', 'CONCESSIONÁRIA', 'LOJA', 'MARCA', 'UF', 'RESPONSÁVEL', 
        'CONSULTORES', 'DATA', 'TMO - DUTO', 'TMO - FREIO', 'TMO - SANIT', 
        'TMO - VERNIZ', 'CX EVAP', 'TMO - TOTAL'
    ]
    
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
    colunas_faltantes = [col for col in COLUNAS_ESPERADAS if col not in df.columns]
    colunas_importantes = ['GRUPO', 'CONCESSIONÁRIA', 'LOJA', 'MARCA', 'UF']
    colunas_importantes_faltantes = [col for col in colunas_importantes if col not in df.columns]
    
    if colunas_importantes_faltantes:
        avisos.append(f"⚠️ Colunas importantes não encontradas: {', '.join(colunas_importantes_faltantes)}")
    
    if colunas_faltantes:
        avisos.append(f"📋 Colunas que serão criadas automaticamente: {', '.join(colunas_faltantes)}")
    
    # Validar colunas numéricas
    colunas_numericas = ['TMO - DUTO', 'TMO - FREIO', 'TMO - SANIT', 'TMO - VERNIZ', 'CX EVAP', 'TMO - TOTAL']
    for col in colunas_numericas:
        if col in df.columns:
            valores_nao_numericos = pd.to_numeric(df[col], errors='coerce').isna().sum()
            if valores_nao_numericos > 0:
                avisos.append(f"⚠️ {valores_nao_numericos} valores não numéricos na coluna '{col}' serão convertidos para 0")
    
    return erros, avisos, linhas_invalidas_detalhes

def processar_consolidacao(df_novo, responsavel, token):
    """Processa a consolidação dos dados - Comparação linha por linha por RESPONSÁVEL + DATA"""
    consolidado_nome = "Reports_Geral_Consolidado.xlsx"

    # Definir colunas esperadas para comparação
    COLUNAS_ESPERADAS = [
        'GRUPO', 'CONCESSIONÁRIA', 'LOJA', 'MARCA', 'UF', 'RESPONSÁVEL', 
        'CONSULTORES', 'DATA', 'TMO - DUTO', 'TMO - FREIO', 'TMO - SANIT', 
        'TMO - VERNIZ', 'CX EVAP', 'TMO - TOTAL'
    ]

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

    # Verificar se colunas esperadas existem na planilha enviada
    colunas_faltantes = [col for col in COLUNAS_ESPERADAS if col not in df_novo.columns]
    if colunas_faltantes:
        st.warning(f"⚠️ Colunas não encontradas na planilha enviada: {', '.join(colunas_faltantes)}")
        # Adicionar colunas faltantes com valores vazios
        for col in colunas_faltantes:
            df_novo[col] = None

    # Calcular TMO - TOTAL se não existir
    if 'TMO - TOTAL' not in df_novo.columns or df_novo['TMO - TOTAL'].isna().all():
        colunas_tmo = ['TMO - DUTO', 'TMO - FREIO', 'TMO - SANIT', 'TMO - VERNIZ']
        colunas_tmo_existentes = [col for col in colunas_tmo if col in df_novo.columns]
        
        if colunas_tmo_existentes:
            df_novo['TMO - TOTAL'] = 0
            for col in colunas_tmo_existentes:
                df_novo[col] = pd.to_numeric(df_novo[col], errors='coerce').fillna(0)
                df_novo['TMO - TOTAL'] += df_novo[col]
            st.info(f"✅ Coluna 'TMO - TOTAL' calculada automaticamente")

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
            
            # Garantir que todas as colunas esperadas existem no consolidado
            for col in COLUNAS_ESPERADAS:
                if col not in df_consolidado.columns:
                    df_consolidado[col] = None
            
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
                    # Reorganizar linha com as colunas na ordem esperada
                    linha_organizada = {}
                    for col in COLUNAS_ESPERADAS:
                        linha_organizada[col] = linha_nova.get(col, None)
                    
                    nova_linha = pd.DataFrame([linha_organizada])
                    df_consolidado = pd.concat([df_consolidado, nova_linha], ignore_index=True)
                    registros_inseridos += 1
                    
                    detalhes_operacoes.append({
                        "Operação": "INSERIR",
                        "Data": data_nova.strftime("%d/%m/%Y"),
                        "Responsável": responsavel_novo,
                        "Observação": "Novo registro"
                    })
                    
                else:
                    # Registro existe - VERIFICAR SE HOUVE ALTERAÇÃO
                    linha_existente = linhas_correspondentes.iloc[0]
                    index_existente = linhas_correspondentes.index[0]
                    
                    # Comparar apenas as colunas esperadas (exceto DATA e RESPONSÁVEL que já foram usadas para busca)
                    colunas_comparacao = [col for col in COLUNAS_ESPERADAS 
                                        if col not in ["DATA", "RESPONSÁVEL"]]
                    
                    houve_alteracao = False
                    campos_alterados = []
                    
                    for col in colunas_comparacao:
                        valor_novo = linha_nova.get(col, None)
                        valor_existente = linha_existente.get(col, None)
                        
                        # Normalizar valores para comparação
                        if pd.isna(valor_novo) and pd.isna(valor_existente):
                            continue  # Ambos são NaN - sem alteração
                        elif pd.isna(valor_novo) or pd.isna(valor_existente):
                            houve_alteracao = True
                            campos_alterados.append(col)
                        else:
                            # Para campos numéricos, converter para float para comparação
                            if col in ['TMO - DUTO', 'TMO - FREIO', 'TMO - SANIT', 'TMO - VERNIZ', 'CX EVAP', 'TMO - TOTAL']:
                                try:
                                    val_novo_num = float(pd.to_numeric(valor_novo, errors='coerce'))
                                    val_exist_num = float(pd.to_numeric(valor_existente, errors='coerce'))
                                    
                                    # Comparar com tolerância para valores float
                                    if abs(val_novo_num - val_exist_num) > 0.001:
                                        houve_alteracao = True
                                        campos_alterados.append(col)
                                except:
                                    # Se não conseguir converter, comparar como string
                                    if str(valor_novo).strip() != str(valor_existente).strip():
                                        houve_alteracao = True
                                        campos_alterados.append(col)
                            else:
                                # Para campos de texto, comparar como string
                                if str(valor_novo).strip().upper() != str(valor_existente).strip().upper():
                                    houve_alteracao = True
                                    campos_alterados.append(col)
                    
                    if houve_alteracao:
                        # ATUALIZAR linha existente
                        for col in COLUNAS_ESPERADAS:
                            if col in linha_nova.index:
                                df_consolidado.loc[index_existente, col] = linha_nova[col]
                        
                        registros_atualizados += 1
                        detalhes_operacoes.append({
                            "Operação": "ATUALIZAR",
                            "Data": data_nova.strftime("%d/%m/%Y"),
                            "Responsável": responsavel_novo,
                            "Observação": f"Campos alterados: {', '.join(campos_alterados[:3])}" + 
                                        ("..." if len(campos_alterados) > 3 else "")
                        })
                    else:
                        # Sem alteração
                        registros_sem_alteracao += 1
                        detalhes_operacoes.append({
                            "Operação": "SEM ALTERAÇÃO",
                            "Data": data_nova.strftime("%d/%m/%Y"),
                            "Responsável": responsavel_novo,
                            "Observação": "Dados idênticos"
                        })
                        
            df_final = df_consolidado.copy()
            
        else:
            # Arquivo consolidado não existe - inserir todos os registros
            # Reorganizar com colunas na ordem esperada
            df_organizado = pd.DataFrame()
            for col in COLUNAS_ESPERADAS:
                df_organizado[col] = df_novo.get(col, None)
            
            df_final = df_organizado.copy()
            registros_inseridos = len(df_novo)
            st.info("📂 Primeiro envio - criando arquivo consolidado")

    # 4. Reorganizar colunas na ordem esperada
    colunas_finais = COLUNAS_ESPERADAS.copy()
    # Adicionar colunas extras que possam existir
    for col in df_final.columns:
        if col not in colunas_finais:
            colunas_finais.append(col)
    
    df_final = df_final.reindex(columns=colunas_finais)
    
    # 5. Ordenar e finalizar
    df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"], na_position='last').reset_index(drop=True)
    
    # Adicionar metadados de controle
    df_final["ÚLTIMA_ATUALIZAÇÃO"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Salvar em buffer
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df_final.to_excel(writer, index=False, sheet_name="Vendas CTs")
    buffer.seek(0)

    # 6. Salvar cópia do arquivo enviado
    salvar_arquivo_enviado(df_novo, responsavel, token)
    
    # 7. Upload do arquivo consolidado
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