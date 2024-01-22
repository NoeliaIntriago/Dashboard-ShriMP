"""
File Name: 2_File_Upload.py

Author: Noelia Intriago (GitHub: NoeliaIntriago)

Creation date: 15/01/2024

Last modified: 21/01/2024

Description:
    This script is part of a Streamlit dashboard application designed for file uploads. It provides functionality 
    for users to upload files related to various categories like exportations, raw materials, shrimp prices, 
    share of wallet, and sales. The script includes the `main` function, setting up the Streamlit dashboard 
    interface, and the `load_files` function, handling the logic for uploading data to a MySQL database.

Dependencies:
    - PIL (for image processing)
    - pandas
    - streamlit (st)
    - mysql.connector (for MySQL database interactions)
    - os, toml (for configuration and path handling)

Functions:
    - load_files(archivo, tabla): Uploads a file to a specified table in the MySQL database.
    - main(): Sets up the Streamlit dashboard for file upload functionality.

Usage: 
    The script is executed to run a specific part of the Streamlit dashboard, allowing users to upload 
    files corresponding to different data categories into a MySQL database.
"""
from PIL import Image
from queries import (
    upload_exportaciones_data,
    upload_materia_prima_data,
    upload_precio_camaron_data,
    upload_sow_data,
    upload_ventas_data,
)

import os
import pandas as pd
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
    """
    Uploads the provided file to the specified table in the MySQL database.

    Depending on the table selected, it calls the respective upload function to handle the file's
    data and insert it into the database. Supports uploading data for different categories such as
    exportations, raw materials, shrimp prices, share of wallet, and sales.

    Parameters:
        - archivo: The file uploaded by the user, expected to be in a format compatible with the selected table.
        - tabla: A string indicating the category of data to upload, which determines the appropriate upload function.

    Returns:
        - A tuple containing the response message and a status code indicating the outcome of the upload process.

    Note:
        - Each upload function requires a specific format for the input file and interacts with a distinct table in the database.
    """
    if tabla == "Exportaciones":
        return upload_exportaciones_data(mysql, archivo)
    elif tabla == "Materia Prima":
        return upload_materia_prima_data(mysql, archivo)
    elif tabla == "Precio Camarón":
        return upload_precio_camaron_data(mysql, archivo)
    elif tabla == "Share of Wallet":
        return upload_sow_data(mysql, archivo)
    elif tabla == "Ventas":
        return upload_ventas_data(mysql, archivo)


def main():
    st.set_page_config(page_title="File Upload", layout="wide", page_icon=":shrimp:")

    with open(path + "/../style/file_upload_style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

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

        if archivo is not None:
            data_load_state = st.info("Cargando datos...", icon="⏳")
            response = load_files(archivo, tabla)

            if response[1] == 500:
                data_load_state.error("Error al cargar los datos", icon="❌")
            elif response[1] == 400:
                data_load_state.warning(response[0], icon="⚠️")
            elif response[1] == 200:
                data_load_state.success("Datos cargados correctamente", icon="✅")


if __name__ == "__main__":
    main()
