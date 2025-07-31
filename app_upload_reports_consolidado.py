from datetime import datetime
import streamlit as st
import pandas as pd
import requests

                            
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