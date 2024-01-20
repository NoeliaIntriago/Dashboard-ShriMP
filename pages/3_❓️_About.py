from PIL import Image

import os
import streamlit as st

path = os.path.dirname(__file__)


def main():
    st.set_page_config(page_title="About ShriMP", layout="wide", page_icon=":shrimp:")

    with open(path + "/../style/style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    logo = Image.open(path + "/../images/logo_ai.png")
    col1, col2 = st.columns([0.5, 2])
    col1.image(logo, width=175)
    col2.title("About ShriMP")

    st.subheader("¿Qué es ShriMP?")
    st.write(
        "ShriMP es un sistema que permite visualizar información de ventas de productos de Expocam. Esta información puede ser filtrada por cliente, producto y etapa de producción."
    )
    st.write(
        "ShriMP también proporciona una herramienta para predecir la producción de balanceado de camarón de Expocam."
    )

    st.divider()

    st.subheader("¿Cómo funciona?")
    st.write(
        "ShriMP se conecta a una base de datos de Expocam para obtener la información de ventas. Esta información es procesada y visualizada en gráficos y tablas."
    )
    st.write(
        "La predicción de producción de balanceado de camarón se realiza utilizando un modelo LSTM entrenado con datos históricos de producción de balanceado de camarón."
    )

    st.divider()

    st.write(
        """
        ShriMP fue desarrollado como parte del proyecto de titulación de la carrera de Ingeniería en Computación de la Escuela Superior Politécnica del Litoral en Guayaquil, Ecuador.
    """
    )

    st.subheader("Desarrolladores")

    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown(
            """
            **Noelia Intriago**  
            *Full-stack Development*  
            [NoeliaIntriago](https://github.com/NoeliaIntriago)"""
        )

    with col2:
        st.markdown(
            """
            **Daniela Landeta**  
            *Data Science*  
            [danielandeta](https://github.com/danielandeta)"""
        )


if __name__ == "__main__":
    main()
