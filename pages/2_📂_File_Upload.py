from PIL import Image
from queries import get_clients, get_historic

import altair as alt
import os
import pandas as pd
import plotly.express as px
import streamlit as st
import toml
import mysql.connector

path = os.path.dirname(__file__)
toml_data = toml.load(path + '/../.streamlit/secrets.toml')

HOST = toml_data['mysql']['host']
DATABASE = toml_data['mysql']['database']
USER = toml_data['mysql']['user']
PASSWORD = toml_data['mysql']['password']
PORT = toml_data['mysql']['port']

mysql = mysql.connector.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)

def main():
    st.set_page_config(page_title='File Upload', layout="wide", page_icon=':shrimp:')

    with open(path+"/../style/style.css") as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

    logo = Image.open(path +"/../images/logo_ai.png")
    col1, col2 = st.columns([0.5, 2])
    col1.image(logo, width=175)
    col2.title("File Upload")

if __name__ == "__main__":
    main()