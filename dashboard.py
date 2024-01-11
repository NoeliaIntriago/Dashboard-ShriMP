from queries import get_clients, get_historic, upload_file
from PIL import Image

import altair as alt
import os
import pandas as pd
import plotly.express as px
import streamlit as st
import toml
import mysql.connector

path = os.path.dirname(__file__)
toml_data = toml.load(path + '/.streamlit/secrets.toml')

HOST = toml_data['mysql']['host']
DATABASE = toml_data['mysql']['database']
USER = toml_data['mysql']['user']
PASSWORD = toml_data['mysql']['password']
PORT = toml_data['mysql']['port']


mysql = mysql.connector.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)

def draw_results(input_data):
    response = get_historic(mysql, input_data)

    if response[1] == 500:
        st.error('Error al cargar los datos', icon="❌")
        return

    if len(response[0]) == 0:
        st.error('No hay datos para mostrar', icon="❌")
        return

    dataframe = pd.DataFrame(response[0])
    dataframe['Fecha'] = pd.to_datetime(dataframe['Fecha'], format='mixed')
    dataframe['Toneladas'] = dataframe['Toneladas'].astype(float)

    bar_df = dataframe.groupby('Producto')['Toneladas'].sum().reset_index().sort_values(by="Toneladas", ascending=False)
    client_df = dataframe.groupby('Cliente')['Toneladas'].sum().reset_index().sort_values(by="Toneladas", ascending=False)

    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.metric(label="Total de ventas", value=str(round(dataframe['Toneladas'].sum(), 2)) + 'Ton' )

    with col2:
        st.metric(label="Ventas promedio por cliente", value=str(round(dataframe['Toneladas'].sum()/len(dataframe['Cliente'].unique()), 2)) + 'Ton')

    with col3:
        st.metric(label="Producto más vendido", value=bar_df.iloc[0]['Producto'])

    with col4:
        st.metric(label="Cliente que más compra", value=client_df.iloc[0]['Cliente'])

    st.divider()
    st.subheader('Tendencia diaria de ventas')
    line_df = dataframe.groupby(dataframe['Fecha'].dt.day)['Toneladas'].sum()
    st.line_chart(line_df)

    col1, col2 = st.columns([1, 1])
    with col1:
        st.subheader('Ventas por producto')
        st.altair_chart(alt.Chart(bar_df).mark_bar().encode(
            x=alt.Y('Producto', sort=None, title='Producto'),
            y=alt.Y('Toneladas', title='Toneladas'),
            tooltip=['Producto', 'Toneladas'],
            color=alt.Color('Producto', legend=None)
        ), use_container_width=True)

    with col2:
        fig = px.sunburst(
                data_frame=dataframe,
                path=[dataframe['Etapa'], dataframe['Producto']],
                color_discrete_sequence=px.colors.qualitative.Alphabet,
            )
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        st.subheader('Ventas por cliente')
        st.altair_chart(alt.Chart(client_df).mark_bar().encode(
            x=alt.Y('Cliente', sort=None, title='Cliente'),
            y=alt.Y('Toneladas', title='Toneladas'),
            tooltip=['Cliente', 'Toneladas'],
            color=alt.Color('Cliente', legend=None)
        ), use_container_width=True)

    with col2:
        fig = px.sunburst(
                data_frame=dataframe,
                path=[dataframe['Cliente'], dataframe['Producto']],
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
        st.plotly_chart(fig, use_container_width=True)

    # DATAFRAME DETALLE DE VENTAS
    st.subheader('Detalle de ventas')
    st.dataframe(
        dataframe,
        use_container_width=True,
        hide_index=True,
        column_order=['Fecha', 'Cliente', 'Producto', 'Familia', 'Etapa', 'Toneladas'],
        column_config={"Fecha": st.column_config.TimeColumn(format="YYYY-MM-DD")},
    )


def main():
    st.set_page_config(page_title='Dashboard ShriMP', layout="wide", page_icon=':shrimp:')

    with open(path+"/style/style.css") as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

    logo = Image.open(path +"/images/logo_ai.png")
    col1, col2 = st.columns([0.5, 2])
    col1.image(logo, width=175)
    col2.title("Dashboard ShriMP")

    input_data = {}

    with st.sidebar:
        with st.form(key='my_form', clear_on_submit=True):
            archivo = st.file_uploader("Archivo de ventas", type=['csv'], accept_multiple_files=False)

            # PREDICTION BUTTON
            predict_button = st.form_submit_button(label='Generar pronóstico')
            if predict_button:
                response = None

                if archivo is not None:
                    data_load_state = st.info('Cargando datos...', icon="ℹ️")
                    data = pd.read_csv(archivo, encoding='latin-1')
                    csv_bytes = data.to_csv().encode()

                    response = upload_file(mysql, csv_bytes)

                    if response[1] == 500:
                        data_load_state.error('Error!', icon="❌")
                    else:
                        data_load_state.success('Éxito!', icon="✅")
                else:
                    st.warning('Debe cargar un archivo', icon="⚠️")

        st.subheader("Búsqueda de datos históricos")

        input_data['year'] = st.number_input("Año", min_value=2020, max_value=2024, step=1)
        input_data['month'] = st.slider("Mes", 1, 12)
        
        input_data['client'] = st.selectbox(
                "Cliente", 
                get_clients(mysql)[0],
                index=None,
            )

        input_data['stage'] = st.radio(
                "Etapa",
                ['SEEDING', 'VOLUMA'],
                index=None,
            )
        
    draw_results(input_data)

if __name__ == "__main__":
    main()