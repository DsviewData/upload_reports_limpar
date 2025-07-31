import streamlit as st
import pandas as pd
import requests
                            
                            # Salvar planilha enviada pelo usuário em subpasta no OneDrive
                            from datetime import datetime
                            nome_pasta = f"Relatorios_Enviados/{responsavel.strip()}_{datetime.now().strftime('%Y-%m-%d')}"
                            nome_arquivo_original = f"{nome_pasta}/{uploaded_file.name}"
                            upload_onedrive(nome_arquivo_original, uploaded_file.getbuffer(), token)

                            sucesso, status, resposta = upload_onedrive(consolidado_nome, buffer.read(), token)
                            if sucesso:
                                st.success("✅ Consolidado atualizado com sucesso!")
                            else:
                                st.error(f"❌ Erro {status}")
                                st.code(resposta)
, token)
                            if sucesso:
                                st.success("✅ Consolidado atualizado com sucesso!")
                            else:
                                st.error(f"❌ Erro {status}")
                                st.code(resposta)

elif aba == "📁 Gerenciar arquivos":
    st.markdown("## 📂 Painel de Arquivos")
    st.divider()
    if token:
        arquivos = listar_arquivos(token)
        if arquivos:
            for arq in arquivos:
                with st.expander(f"📄 {arq['name']}"):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.markdown(f"[🔗 Acessar arquivo]({arq['@microsoft.graph.downloadUrl']})")
                        st.write(f"Tamanho: {round(arq['size']/1024, 2)} KB")
        else:
            st.info("Nenhum arquivo encontrado na pasta uploads.")
    else:
        st.error("Erro ao autenticar.")