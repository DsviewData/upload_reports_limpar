def validar_dados_enviados(df, responsavel):
    """Valida os dados enviados pelo usuário"""
    erros = []
    avisos = []
    
    # Validar responsável
    if not responsavel or not responsavel.strip():
        erros.append("⚠️ O nome do responsável é obrigatório")
    elif len(responsavel.strip()) < 2:
        erros.append("⚠️ O nome do responsável deve ter pelo menos 2 caracteres")
    
    # Validar se DataFrame não está vazio
    if df.empty:
        erros.append("❌ A planilha está vazia")
        return erros, avisos
    
    # Validar se existe coluna DATA
    if "DATA" not in df.columns:
        erros.append("⚠️ A planilha deve conter uma coluna 'DATA'")
        avisos.append("📋 Lembre-se: o arquivo deve ter uma aba chamada 'Vendas CTs' com a coluna 'DATA'")
    else:
        # Validar se as datas são válidas
        df_temp = df.copy()
        df_temp["DATA_CONVERTIDA"] = pd.to_datetime(df_temp["DATA"], errors="coerce")
        
        # Identificar linhas com datas inválidas
        linhas_invalidas = df_temp[df_temp["DATA_CONVERTIDA"].isna()]
        datas_validas = df_temp["DATA_CONVERTIDA"].notna().sum()
        
        if datas_validas == 0:
            erros.append("❌ Nenhuma data válida encontrada na coluna 'DATA'")
        elif len(linhas_invalidas) > 0:
            # Mostrar detalhes das linhas com datas inválidas
            detalhes_invalidas = []
            for idx, row in linhas_invalidas.iterrows():
                linha_excel = idx + 2  # +2 porque Excel começa em 1 e tem cabeçalho
                valor_data = row["DATA"]
                detalhes_invalidas.append(f"Linha {linha_excel}: '{valor_data}'")
            
            # Limitar a exibição para não sobrecarregar a tela
            if len(detalhes_invalidas) <= 10:
                avisos.append(f"⚠️ **{len(linhas_invalidas)} linhas com datas inválidas encontradas:**")
                for detalhe in detalhes_invalidas:
                    avisos.append(f"   • {detalhe}")
                avisos.append("❗ **IMPORTANTE:** Estas linhas serão **IGNORADAS** na consolidação. Corrija as datas para incluí-las nos dados consolidados.")
            else:
                avisos.append(f"⚠️ **{len(linhas_invalidas)} linhas com datas inválidas encontradas:**")
                # Mostrar apenas as primeiras 10
                for detalhe in detalhes_invalidas[:10]:
                    avisos.append(f"   • {detalhe}")
                avisos.append(f"   • ... e mais {len(detalhes_invalidas) - 10} linhas")
                avisos.append("❗ **IMPORTANTE:** Estas linhas serão **IGNORADAS** na consolidação. Corrija as datas para incluí-las nos dados consolidados.")
    
    # Validar duplicatas na planilha enviada
    if not df.empty and "DATA" in df.columns:
        df_temp = df.copy()
        df_temp["DATA"] = pd.to_datetime(df_temp["DATA"], errors="coerce")
        df_temp = df_temp.dropna(subset=["DATA"])
        
        if not df_temp.empty:
            # Identificar duplicatas específicas
            duplicatas_mask = df_temp.duplicated(subset=["DATA"], keep=False)
            if duplicatas_mask.any():
                linhas_duplicadas = df_temp[duplicatas_mask]
                datas_duplicadas = linhas_duplicadas["DATA"].dt.strftime("%d/%m/%Y").value_counts()
                
                detalhes_duplicatas = []
                for data, count in datas_duplicadas.items():
                    linhas_com_data = df_temp[df_temp["DATA"].dt.strftime("%d/%m/%Y") == data].index + 2
                    linhas_str = ", ".join([str(linha) for linha in linhas_com_data])
                    detalhes_duplicatas.append(f"{data} (linhas: {linhas_str})")
                
                avisos.append(f"⚠️ **Datas duplicadas encontradas:**")
                for detalhe in detalhes_duplicatas[:5]:  # Limitar a 5 para não sobrecarregar
                    avisos.append(f"   • {detalhe}")
                if len(detalhes_duplicatas) > 5:
                    avisos.append(f"   • ... e mais {len(detalhes_duplicatas) - 5} datas duplicadas")
    
    return erros, avisos

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
    
    # Converter datas e identificar linhas inválidas ANTES de removê-las
    df_novo["DATA_ORIGINAL"] = df_novo["DATA"]  # Guardar valor original
    df_novo["DATA"] = pd.to_datetime(df_novo["DATA"], errors="coerce")
    
    # Identificar e mostrar linhas inválidas que serão removidas
    linhas_invalidas_mask = df_novo["DATA"].isna()
    linhas_invalidas = df_novo[linhas_invalidas_mask]
    
    if len(linhas_invalidas) > 0:
        st.warning(f"🧹 **{len(linhas_invalidas)} linhas serão removidas por terem datas inválidas:**")
        
        # Criar uma tabela mostrando as linhas problemáticas
        linhas_problema = linhas_invalidas[["DATA_ORIGINAL"]].copy()
        linhas_problema.index = linhas_problema.index + 2  # Ajustar para numeração do Excel
        linhas_problema.index.name = "Linha no Excel"
        linhas_problema.columns = ["Data Inválida"]
        
        # Mostrar tabela com as linhas problemáticas (limitando a 20 registros)
        if len(linhas_problema) <= 20:
            st.dataframe(linhas_problema, use_container_width=True)
        else:
            st.dataframe(linhas_problema.head(20), use_container_width=True)
            st.info(f"... e mais {len(linhas_problema) - 20} linhas com problemas similares")
        
        st.error("❗ **ATENÇÃO:** Estas linhas NÃO serão incluídas nos dados consolidados. Corrija as datas e envie novamente para incluí-las.")

    # Remover linhas inválidas
    df_novo = df_novo.dropna(subset=["DATA"])
    df_novo = df_novo.drop(columns=["DATA_ORIGINAL"])  # Remover coluna auxiliar

    if df_novo.empty:
        st.error("❌ Nenhum registro válido para consolidar")
        return False

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