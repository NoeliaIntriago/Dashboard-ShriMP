from PIL import Image
from queries import (
    upload_exportaciones_data,
    upload_materia_prima_data,
    upload_precio_camaron_data,
    upload_sow_data,
    upload_ventas_data,
)

import os
import streamlit as st
import toml
import mysql.connector

path = os.path.dirname(__file__)
toml_data = toml.load(path + "/../.streamlit/secrets.toml")

HOST = toml_data["mysql"]["host"]
DATABASE = toml_data["mysql"]["database"]
USER = toml_data["mysql"]["user"]
PASSWORD = toml_data["mysql"]["password"]
PORT = toml_data["mysql"]["port"]

mysql = mysql.connector.connect(
    host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT
)


def load_files(archivo, tabla):
    if tabla == "Exportaciones":
        upload_exportaciones_data(mysql, archivo)
    elif tabla == "Materia Prima":
        upload_materia_prima_data(mysql, archivo)
    elif tabla == "Precio Camarón":
        upload_precio_camaron_data(mysql, archivo)
    elif tabla == "Share of Wallet":
        upload_sow_data(mysql, archivo)
    elif tabla == "Ventas":
        upload_ventas_data(mysql, archivo)


def main():
    st.set_page_config(page_title="File Upload", layout="wide", page_icon=":shrimp:")

    with open(path + "/../style/style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    logo = Image.open(path + "/../images/logo_ai.png")
    col1, col2 = st.columns([0.5, 2])
    col1.image(logo, width=175)
    col2.title("File Upload")

    form = st.form(key="my_form", clear_on_submit=True)
    with form:
        col1, col2 = st.columns([1, 2], gap="large")

        tabla = col1.selectbox(
            "Seleccione los datos a cargar",
            [
                "Exportaciones",
                "Materia Prima",
                "Precio Camarón",
                "Share of Wallet",
                "Ventas",
            ],
        )
        archivo = col2.file_uploader(
            "Seleccione el archivo a cargar", type=["xlsx"], accept_multiple_files=False
        )
        subir_archivo = col2.form_submit_button("Subir archivo")

        if subir_archivo is not None:
            response = None
            if archivo is not None:
                data_load_state = st.info("Cargando datos...", icon="⏳")
                response = load_files(archivo, tabla)

                if response[1] == 500:
                    data_load_state.error("Error al cargar los datos", icon="❌")
                if response[1] == 200:
                    data_load_state.success("Datos cargados correctamente", icon="✅")
                return
            else:
                col1.info("Seleccione un archivo para cargar los datos", icon="⚠️")


if __name__ == "__main__":
    main()
