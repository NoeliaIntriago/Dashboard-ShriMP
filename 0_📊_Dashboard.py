"""
File Name: 0_Dashboard.py

Author: Noelia Intriago (GitHub: NoeliaIntriago)

Creation date: 11/01/2024

Last modified: 21/01/2024

Description:
    This Python script is central to a Streamlit dashboard application, tailored for visualizing 
    sales data. It includes the `main` function, which sets up the dashboard's layout, styling, and 
    interactivity, and the `draw_results` function for detailed data processing and visualization. 
    The script handles user inputs, queries data from a MySQL database, and uses libraries like 
    Pandas, Altair, and Plotly for data manipulation and visualization.

Dependencies:
    - pandas
    - streamlit
    - altair
    - plotly.express
    - MySQL connector/library
    - PIL (for image processing)

Functions:
    - main(): Sets up the Streamlit dashboard layout, styles, and interactive components.
    - draw_results(input_data): Processes and visualizes sales data based on user inputs.

Usage: 
    Execute this script to run the Streamlit dashboard application. The application provides 
    interactive visualizations of sales data, assisting in data-driven decision-making.
"""
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
toml_data = toml.load(path + "/.streamlit/secrets.toml")

HOST = toml_data["mysql"]["host"]
DATABASE = toml_data["mysql"]["database"]
USER = toml_data["mysql"]["user"]
PASSWORD = toml_data["mysql"]["password"]
PORT = toml_data["mysql"]["port"]

mysql = mysql.connector.connect(
    host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT
)


def draw_results(input_data):
    """
    Draws and displays the results of sales data in various visual formats on a Streamlit dashboard.

    This function retrieves historical sales data based on the provided input data, processes it, and
    displays a series of visualizations and metrics. These include bar charts for sales by product and
    client, a line chart for daily sales trends, a sunburst chart for product sales by stage, and a
    detailed sales data table.

    Parameters:
        - input_data: A data structure containing the necessary parameters for the sales data query. The exact structure is dependent on the database schema and query requirements.

    Side Effects:
        - Calls the `get_historic` function to retrieve sales data from a MySQL database.
        - Uses Streamlit (st) to render error messages, metrics, charts, and tables directly to the dashboard.
        - Processes and manipulates the data using pandas.

    Note:
        - This function does not return any value. All results are displayed directly on the Streamlit dashboard.
        - It handles error scenarios by displaying appropriate error messages on the dashboard.

    Dependencies:
        - pandas (as pd)
        - streamlit (as st)
        - altair (as alt)
        - plotly.express (as px)
    """
    response = get_historic(mysql, input_data)

    if response[1] == 500:
        st.error("Error al cargar los datos", icon="❌")
        return

    if len(response[0]) == 0:
        st.error("No hay datos para mostrar", icon="❌")
        return

    dataframe = pd.DataFrame(response[0])
    dataframe["Fecha"] = pd.to_datetime(dataframe["Fecha"], format="mixed")
    dataframe["Toneladas"] = dataframe["Toneladas"].astype(float)

    bar_df = (
        dataframe.groupby("Producto")["Toneladas"]
        .sum()
        .reset_index()
        .sort_values(by="Toneladas", ascending=False)
    )
    client_df = (
        dataframe.groupby("Cliente")["Toneladas"]
        .sum()
        .reset_index()
        .sort_values(by="Toneladas", ascending=False)
    )

    st.subheader("Resumen de ventas")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        st.metric(
            label="Total de ventas",
            value=str(round(dataframe["Toneladas"].sum(), 2)) + " Ton",
        )

    with col2:
        st.metric(
            label="Ventas promedio por cliente",
            value=str(
                round(
                    dataframe["Toneladas"].sum() / len(dataframe["Cliente"].unique()), 2
                )
            )
            + " Ton",
        )

    with col3:
        st.metric(label="Producto más vendido", value=bar_df.iloc[0]["Producto"])

    with col4:
        st.metric(label="Cliente que más compra", value=client_df.iloc[0]["Cliente"])

    tab1, tab2 = st.tabs(["Tendencia", "Detalle de ventas"])

    with tab1:
        st.subheader("Tendencia diaria de ventas")
        line_df = dataframe.groupby(dataframe["Fecha"].dt.day)["Toneladas"].sum()
        st.line_chart(line_df)

    with tab2:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.subheader("Ventas por producto")
            st.altair_chart(
                alt.Chart(bar_df)
                .mark_bar()
                .encode(
                    x=alt.Y("Producto", sort=None, title="Producto"),
                    y=alt.Y("Toneladas", title="Toneladas"),
                    tooltip=["Producto", "Toneladas"],
                    color=alt.Color("Producto", legend=None),
                ),
                use_container_width=True,
            )

        with col2:
            top_6_etapa = (
                dataframe.groupby(["Etapa", "Producto"])["Toneladas"]
                .sum()
                .reset_index()
                .sort_values(by="Toneladas", ascending=False)
                .head(6)
            )
            fig = px.sunburst(
                data_frame=dataframe,
                path=[top_6_etapa["Etapa"], top_6_etapa["Producto"]],
                color_discrete_sequence=px.colors.qualitative.Alphabet,
            )
            st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns([1, 1])
        with col1:
            st.subheader("Ventas por cliente")
            st.altair_chart(
                alt.Chart(client_df)
                .mark_bar()
                .encode(
                    x=alt.Y("Cliente", sort=None, title="Cliente"),
                    y=alt.Y("Toneladas", title="Toneladas"),
                    tooltip=["Cliente", "Toneladas"],
                    color=alt.Color("Cliente", legend=None),
                ),
                use_container_width=True,
            )

        with col2:
            top_6_clientes = (
                dataframe.groupby(["Cliente", "Producto"])["Toneladas"]
                .sum()
                .reset_index()
                .sort_values(by="Toneladas", ascending=False)
                .head(6)
            )
            fig = px.sunburst(
                data_frame=dataframe,
                path=[top_6_clientes["Cliente"], top_6_clientes["Producto"]],
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            st.plotly_chart(fig, use_container_width=True)

        # DATAFRAME DETALLE DE VENTAS
        st.subheader("Detalle de ventas")
        st.dataframe(
            dataframe,
            use_container_width=True,
            hide_index=True,
            column_order=[
                "Fecha",
                "Cliente",
                "Producto",
                "Familia",
                "Etapa",
                "Toneladas",
            ],
            column_config={"Fecha": st.column_config.TimeColumn(format="YYYY-MM-DD")},
        )


def main():
    """
    Main function of the Streamlit dashboard application. It sets up the page configuration,
    applies custom CSS styles, and defines the layout and interactive components of the dashboard.

    This function initializes the dashboard with a custom title, layout settings, and a logo. It
    creates interactive widgets for filtering the sales data, such as year and month selectors, client
    and stage choice inputs. These filters are used to fetch and display relevant sales data using
    the `draw_results` function.

    Side Effects:
        - Configures the Streamlit page with a title, layout, and icon.
        - Reads and applies custom CSS styles to the dashboard.
        - Displays the dashboard elements like title, filters, and calls the `draw_results` function to render the sales data visualizations based on user inputs.
    """
    st.set_page_config(
        page_title="ShriMP Dashboard", layout="wide", page_icon=":shrimp:"
    )

    with open(path + "/style/dashboard_style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    with open(path + "/style/style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    logo = Image.open(path + "/images/logo_ai.png")
    col1, col2 = st.columns([0.5, 2])
    col1.image(logo, width=175)
    col2.title("ShriMP Dashboard")

    input_data = {}

    st.subheader("Filtros")
    col1, col2, col3 = st.columns([1, 1, 1], gap="large")

    input_data["year"] = col1.number_input(
        "Año", min_value=2020, max_value=2024, step=1
    )
    input_data["month"] = col1.slider("Mes", 1, 12)

    input_data["client"] = col2.selectbox(
        "Cliente",
        get_clients(mysql)[0],
        index=None,
    )

    input_data["stage"] = col2.radio(
        "Etapa",
        ["SEEDING", "VOLUMA"],
        index=None,
    )

    st.divider()

    draw_results(input_data)


if __name__ == "__main__":
    main()
