from PIL import Image

import os
import streamlit as st

path = os.path.dirname(__file__)

def main():
    st.set_page_config(page_title='About ShriMP', layout="wide", page_icon=':shrimp:')

    with open(path+"/../style/style.css") as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

    logo = Image.open(path +"/../images/logo_ai.png")
    col1, col2 = st.columns([0.5, 2])
    col1.image(logo, width=175)
    col2.title('About ShriMP')
 
    st.write('''
        ShriMP fue desarrollado como parte del proyecto de titulación de la carrera de Ingeniería en Computación de la Escuela Superior Politécnica del Litoral.
    ''')

    st.divider()

    st.subheader('Desarrolladores')

    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown('''
            **Noelia Intriago**  
            *Full-stack Development*  
            [NoeliaIntriago](https://github.com/NoeliaIntriago)''')
        
    with col2:
        st.markdown('''
            **Daniela Landeta**  
            *Data Science*  
            [danielandeta](https://github.com/danielandeta)''')
                    


if __name__ == "__main__":
    main()