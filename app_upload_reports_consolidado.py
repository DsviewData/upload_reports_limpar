import pandas as pd
import streamlit as st
from datetime import datetime
import hashlib

def gerar_hash_registro(row, colunas_chave):
    """Gera hash único para um registro baseado nas colunas-chave"""
    valores = []
    for col in colunas_chave:
        if col in row.index:
            valor = str(row[col]).strip().upper() if pd.notna(row[col]) else "NULL"
            valores.append(valor)
    
    texto_concatenado = "|".join(valores)
    return hashlib.md5(texto_concatenado.encode()).hexdigest()

def definir_colunas_chave(df):
    """Define colunas-chave para identificação de registros únicos"""
    colunas_obrigatorias = ["DATA", "RESPONSÁVEL"]
    colunas_opcionais = []
    
    # Buscar por colunas que podem ser identificadores únicos
    colunas_candidatas = [
        "ID", "CODIGO", "NUMERO", "REFERENCIA", "CLIENTE", 
        "PRODUTO", "SERVICO", "CONTA", "CPF", "CNPJ"
    ]
    
    for col in df.columns:
        col_upper = col.upper()
        if any(candidata in col_upper for candidata in colunas_candidatas):
            colunas_opcionais.append(col)
    
    # Priorizar colunas com alta cardinalidade
    for col in df.columns:
        if col not in colunas_obrigatorias and col not in colunas_opcionais:
            if df[col].nunique() / len(df) > 0.8:  # Alta variabilidade
                colunas_opcionais.append(col)
    
    return colunas_obrigatorias + colunas_opcionais[:3]  # Máximo 5 colunas-chave

def analisar_duplicatas_inteligente(df_novo, df_consolidado, responsavel):
    """Análise inteligente de duplicatas e conflitos"""
    
    # 1. Definir colunas-chave para comparação
    colunas_chave = definir_colunas_chave(df_novo)
    
    # 2. Gerar hashes para registros novos
    df_novo_copy = df_novo.copy()
    df_novo_copy['HASH_REGISTRO'] = df_novo_copy.apply(
        lambda row: gerar_hash_registro(row, colunas_chave), axis=1
    )
    
    # 3. Gerar hashes para registros consolidados (se existir)
    if not df_consolidado.empty:
        df_consolidado_copy = df_consolidado.copy()
        df_consolidado_copy['HASH_REGISTRO'] = df_consolidado_copy.apply(
            lambda row: gerar_hash_registro(row, colunas_chave), axis=1
        )
    else:
        df_consolidado_copy = pd.DataFrame()
    
    # 4. Análise de duplicatas
    resultado_analise = {
        'registros_novos': len(df_novo_copy),
        'registros_consolidados': len(df_consolidado_copy),
        'colunas_chave_usadas': colunas_chave,
        'duplicatas_internas': 0,
        'conflitos_encontrados': 0,
        'registros_a_substituir': 0,
        'registros_realmente_novos': 0,
        'detalhes_conflitos': []
    }
    
    # 5. Verificar duplicatas internas no arquivo novo
    duplicatas_internas = df_novo_copy['HASH_REGISTRO'].duplicated()
    resultado_analise['duplicatas_internas'] = duplicatas_internas.sum()
    
    if duplicatas_internas.any():
        st.warning(f"⚠️ {duplicatas_internas.sum()} registros duplicados encontrados no arquivo enviado")
        # Remover duplicatas internas, mantendo o último
        df_novo_copy = df_novo_copy.drop_duplicates(subset=['HASH_REGISTRO'], keep='last')
    
    # 6. Comparar com dados consolidados
    if not df_consolidado_copy.empty:
        # Identificar registros que já existem
        hashes_existentes = set(df_consolidado_copy['HASH_REGISTRO'])
        hashes_novos = set(df_novo_copy['HASH_REGISTRO'])
        
        # Registros que são exatamente iguais (sem conflito)
        registros_identicos = hashes_novos.intersection(hashes_existentes)
        
        # Registros realmente novos
        registros_realmente_novos = hashes_novos - hashes_existentes
        resultado_analise['registros_realmente_novos'] = len(registros_realmente_novos)
        
        # Verificar conflitos por responsável/data (lógica adicional)
        conflitos_responsavel_data = verificar_conflitos_responsavel_data(
            df_novo_copy, df_consolidado_copy, responsavel
        )
        
        resultado_analise['conflitos_encontrados'] = len(conflitos_responsavel_data)
        resultado_analise['detalhes_conflitos'] = conflitos_responsavel_data
        
        # Registros a serem substituídos (mesmo responsável, mesma data, dados diferentes)
        resultado_analise['registros_a_substituir'] = len(conflitos_responsavel_data)
        
    else:
        resultado_analise['registros_realmente_novos'] = len(df_novo_copy)
    
    return df_novo_copy, resultado_analise

def verificar_conflitos_responsavel_data(df_novo, df_consolidado, responsavel):
    """Verifica conflitos específicos por responsável e data"""
    conflitos = []
    
    # Filtrar apenas registros do mesmo responsável no consolidado
    df_mesmo_responsavel = df_consolidado[
        df_consolidado['RESPONSÁVEL'] == responsavel
    ].copy()
    
    if df_mesmo_responsavel.empty:
        return conflitos
    
    # Comparar por data
    for _, row_novo in df_novo.iterrows():
        data_novo = pd.to_datetime(row_novo['DATA']).normalize()
        
        # Buscar registros na mesma data
        registros_mesma_data = df_mesmo_responsavel[
            pd.to_datetime(df_mesmo_responsavel['DATA']).dt.normalize() == data_novo
        ]
        
        for _, row_existente in registros_mesma_data.iterrows():
            if row_novo['HASH_REGISTRO'] != row_existente['HASH_REGISTRO']:
                conflito = {
                    'data': data_novo.strftime('%d/%m/%Y'),
                    'responsavel': responsavel,
                    'tipo': 'dados_diferentes_mesma_data',
                    'registro_novo': row_novo.to_dict(),
                    'registro_existente': row_existente.to_dict()
                }
                conflitos.append(conflito)
    
    return conflitos

def processar_consolidacao_inteligente(df_novo, df_consolidado, responsavel):
    """Processamento inteligente de consolidação"""
    
    try:
        # 1. Preparar dados
        df_novo = df_novo.copy()
        df_novo["RESPONSÁVEL"] = responsavel.strip()
        df_novo.columns = df_novo.columns.str.strip().str.upper()
        
        if not df_consolidado.empty:
            df_consolidado.columns = df_consolidado.columns.str.strip().str.upper()
        
        # 2. Processar datas
        df_novo["DATA"] = pd.to_datetime(df_novo["DATA"], errors="coerce")
        df_novo = df_novo.dropna(subset=["DATA"])
        
        if df_novo.empty:
            return None, None, "❌ Nenhuma data válida encontrada na planilha enviada."
        
        if not df_consolidado.empty:
            df_consolidado["DATA"] = pd.to_datetime(df_consolidado["DATA"], errors="coerce")
            df_consolidado = df_consolidado.dropna(subset=["DATA"])
        
        # 3. Análise inteligente de duplicatas
        df_novo_processado, analise = analisar_duplicatas_inteligente(
            df_novo, df_consolidado, responsavel
        )
        
        # 4. Processar conforme análise
        if analise['conflitos_encontrados'] > 0:
            # Remover registros conflitantes do consolidado
            df_consolidado_limpo = remover_registros_conflitantes(
                df_consolidado, analise['detalhes_conflitos']
            )
        else:
            df_consolidado_limpo = df_consolidado.copy()
        
        # 5. Consolidar dados
        # Remover coluna de hash antes da consolidação final
        df_novo_final = df_novo_processado.drop('HASH_REGISTRO', axis=1)
        
        if not df_consolidado_limpo.empty and 'HASH_REGISTRO' in df_consolidado_limpo.columns:
            df_consolidado_limpo = df_consolidado_limpo.drop('HASH_REGISTRO', axis=1)
        
        df_final = pd.concat([df_consolidado_limpo, df_novo_final], ignore_index=True)
        
        # 6. Ordenar e limpar
        df_final = df_final.sort_values(["DATA", "RESPONSÁVEL"]).reset_index(drop=True)
        
        return df_final, analise, None
        
    except Exception as e:
        return None, None, f"❌ Erro na consolidação inteligente: {str(e)}"

def remover_registros_conflitantes(df_consolidado, conflitos):
    """Remove registros conflitantes do DataFrame consolidado"""
    df_limpo = df_consolidado.copy()
    
    for conflito in conflitos:
        # Encontrar e remover o registro existente que está em conflito
        registro_existente = conflito['registro_existente']
        
        # Criar máscara para encontrar o registro exato
        mascara = True
        for coluna, valor in registro_existente.items():
            if coluna in df_limpo.columns and coluna != 'HASH_REGISTRO':
                if pd.notna(valor):
                    mascara = mascara & (df_limpo[coluna] == valor)
                else:
                    mascara = mascara & df_limpo[coluna].isna()
        
        # Remover registros que correspondem à máscara
        df_limpo = df_limpo[~mascara]
    
    return df_limpo

def exibir_relatorio_consolidacao(analise):
    """Exibe relatório detalhado da consolidação"""
    
    st.markdown("### 📊 Relatório de Consolidação")
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📥 Registros Enviados", analise['registros_novos'])
    
    with col2:
        st.metric("📁 No Consolidado", analise['registros_consolidados'])
    
    with col3:
        st.metric("✅ Realmente Novos", analise['registros_realmente_novos'])
    
    with col4:
        st.metric("🔄 Substituições", analise['registros_a_substituir'])
    
    # Detalhes adicionais
    if analise['duplicatas_internas'] > 0:
        st.warning(f"⚠️ **{analise['duplicatas_internas']} duplicatas internas** foram removidas do arquivo enviado")
    
    if analise['conflitos_encontrados'] > 0:
        st.info(f"🔄 **{analise['conflitos_encontrados']} registros** serão substituídos por conterem dados diferentes para as mesmas chaves")
        
        with st.expander("📋 Detalhes dos Conflitos"):
            for i, conflito in enumerate(analise['detalhes_conflitos']):
                st.write(f"**Conflito {i+1}:**")
                st.write(f"- Data: {conflito['data']}")
                st.write(f"- Responsável: {conflito['responsavel']}")
                st.write(f"- Tipo: {conflito['tipo']}")
                st.divider()
    
    # Colunas-chave utilizadas
    st.markdown("#### 🔑 Colunas-chave para identificação:")
    st.write(", ".join(analise['colunas_chave_usadas']))
    
    return True

# Exemplo de integração no código principal
def exemplo_uso_consolidacao():
    """Exemplo de como integrar no código principal"""
    
    # Substituir a função processar_consolidacao original por:
    df_final, analise, erro = processar_consolidacao_inteligente(
        df_novo, df_consolidado, responsavel
    )
    
    if erro:
        st.error(erro)
        return
    
    # Exibir relatório antes de confirmar
    if exibir_relatorio_consolidacao(analise):
        # Mostrar preview do resultado final
        st.markdown("### 👀 Preview do Resultado Final")
        st.dataframe(df_final.tail(10), use_container_width=True)
        
        # Confirmar operação
        if st.button("✅ Confirmar Consolidação"):
            # Prosseguir com o upload...
            pass