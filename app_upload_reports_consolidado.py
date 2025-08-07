def comparar_e_atualizar_registros_v2(df_consolidado, df_novo):
    """
    Nova lógica de consolidação:
    - Para cada RESPONSÁVEL e DATA no arquivo novo
    - Remove TODOS os registros existentes com mesmo RESPONSÁVEL e DATA
    - Insere os novos registros
    """
    registros_inseridos = 0
    registros_substituidos = 0
    registros_removidos = 0
    detalhes_operacao = []
    
    if df_consolidado.empty:
        # Primeiro envio - todos os registros são novos
        df_final = df_novo.copy()
        registros_inseridos = len(df_novo)
        
        for _, row in df_novo.iterrows():
            detalhes_operacao.append({
                "Operação": "INSERIDO",
                "Responsável": row["RESPONSÁVEL"],
                "Data": row["DATA"].strftime("%d/%m/%Y"),
                "Motivo": "Primeiro envio"
            })
        
        return df_final, registros_inseridos, registros_substituidos, registros_removidos, detalhes_operacao
    
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
            # SUBSTITUIÇÃO: Remover registros existentes
            num_removidos = len(registros_existentes)
            df_final = df_final[~mask_existente]
            registros_removidos += num_removidos
            
            # Adicionar detalhes da remoção
            detalhes_operacao.append({
                "Operação": "REMOVIDO",
                "Responsável": responsavel,
                "Data": data_grupo.strftime("%d/%m/%Y"),
                "Motivo": f"{num_removidos} registro(s) antigo(s) removido(s)"
            })
            
            registros_substituidos += len(grupo_df)
            operacao_tipo = "SUBSTITUÍDO"
        else:
            # INSERÇÃO: Não havia registros anteriores
            registros_inseridos += len(grupo_df)
            operacao_tipo = "INSERIDO"
        
        # Inserir novos registros
        df_final = pd.concat([df_final, grupo_df], ignore_index=True)
        
        # Adicionar detalhes da operação
        detalhes_operacao.append({
            "Operação": operacao_tipo,
            "Responsável": responsavel,
            "Data": data_grupo.strftime("%d/%m/%Y"),
            "Motivo": f"{len(grupo_df)} registro(s) processado(s)"
        })
    
    return df_final, registros_inseridos, registros_substituidos, registros_removidos, detalhes_operacao


def processar_consolidacao_v2(df_novo, nome_arquivo, token):
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
    
    st.info(f"👥 **Responsáveis:** {', '.join(responsaveis_no_envio)}")
    st.info(f"📅 **Período:** {periodo_min} até {periodo_max}")
    
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
        df_final, inseridos, substituidos, removidos, detalhes = comparar_e_atualizar_registros_v2(
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
            st.metric("➕ Inseridos", inseridos)
        with col3:
            st.metric("🔄 Substituídos", substituidos)
        with col4:
            st.metric("🗑️ Removidos", removidos)
        
        # Detalhes das operações
        if detalhes:
            with st.expander("📋 Detalhes das Operações", expanded=removidos > 0):
                df_detalhes = pd.DataFrame(detalhes)
                st.dataframe(df_detalhes, use_container_width=True, hide_index=True)
        
        # Resumo por responsável
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


def criar_backup_substituicoes(df_consolidado, detalhes_operacao, token):
    """
    Cria backup dos registros que foram substituídos
    """
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


# Função para substituir a atual no main()
def main_melhorado():
    """
    Versão do main() com a nova lógica de consolidação
    """
    # ... (manter todo o código do main atual até a parte do botão de consolidação)
    
    # Substituir apenas esta parte:
    if st.button("📧 Consolidar Dados", type="primary", disabled=bool(erros)):
        if erros:
            st.error("❌ Corrija os erros acima antes de prosseguir")
        else:
            # USAR A NOVA FUNÇÃO:
            sucesso = processar_consolidacao_v2(df, uploaded_file.name, token)
            if sucesso:
                st.balloons()