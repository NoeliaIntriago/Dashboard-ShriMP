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
import plotly.graph_objects as go
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


def build_hierarchical_dataframe(df, levels, value_column):
    """
    Build a hierarchy of levels for Sunburst or Treemap charts.

    Levels are given starting from the bottom to the top of the hierarchy,
    ie the last level corresponds to the root.
    """
    df_all_trees = pd.DataFrame(columns=["id", "parent", "value"])
    for i, level in enumerate(levels):
        df_tree = pd.DataFrame(columns=["id", "parent", "value"])
        dfg = df.groupby(levels[i:]).sum()
        dfg = dfg.reset_index()
        df_tree["id"] = dfg[level].copy()
        if i < len(levels) - 1:
            df_tree["parent"] = dfg[levels[i + 1]].copy()
        else:
            df_tree["parent"] = "Cliente"
        df_tree["value"] = dfg[value_column]
        df_all_trees = pd.concat([df_all_trees, df_tree], ignore_index=True)
    total = pd.Series(
        dict(
            id="Cliente",
            parent="",
            value=df[value_column].sum(),
        )
    )
    df_all_trees = pd.concat([df_all_trees, pd.DataFrame([total])], ignore_index=True)
    return df_all_trees


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

    if response[2] == 500:
        st.error("Error al cargar los datos", icon="❌")
        return

    if len(response[0]) == 0:
        st.error("No hay datos para mostrar", icon="❌")
        return

    dataframe_actual = pd.DataFrame(response[0])
    dataframe_pasado = []

    dataframe_actual["Fecha"] = pd.to_datetime(
        dataframe_actual["Fecha"], format="mixed"
    )
    dataframe_actual["Toneladas"] = dataframe_actual["Toneladas"].astype(float)
    total_venta_actual = dataframe_actual["Toneladas"].sum()
    total_venta_pasado = 0
    diferencia_meses = 0

    if len(response[1]) != 0:
        dataframe_pasado = pd.DataFrame(response[1])
        dataframe_pasado["Fecha"] = pd.to_datetime(
            dataframe_pasado["Fecha"], format="mixed"
        )
        dataframe_pasado["Toneladas"] = dataframe_pasado["Toneladas"].astype(float)
        total_venta_pasado = dataframe_pasado["Toneladas"].sum()
        diferencia_meses = (
            total_venta_actual - total_venta_pasado
        ) / total_venta_pasado

    promedio_ventas = dataframe_actual["Toneladas"].mean()
    diferencia_absoluta = total_venta_actual - promedio_ventas
    porcentaje_ventas = 0

    if total_venta_actual != 0:
        porcentaje_ventas = (diferencia_absoluta / total_venta_actual) * 100

    bar_df = (
        dataframe_actual.groupby("Producto")["Toneladas"]
        .sum()
        .reset_index()
        .sort_values(by="Toneladas", ascending=False)
    )
    client_df = (
        dataframe_actual.groupby("Cliente")["Toneladas"]
        .sum()
        .reset_index()
        .sort_values(by="Toneladas", ascending=False)
    )

    st.subheader("Resumen de ventas")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        st.metric(
            label="Total de ventas",
            value=str(int(dataframe_actual["Toneladas"].sum())) + " Ton",
            delta=str(round(diferencia_meses * 100, 2)) + "%",
        )

    with col2:
        st.metric(
            label="Total vs. Promedio de ventas",
            value=str(round(porcentaje_ventas, 2)) + " %",
        )

    with col3:
        st.metric(label="Producto más vendido", value=bar_df.iloc[0]["Producto"])

    with col4:
        st.metric(label="Cliente que más compra", value=client_df.iloc[0]["Cliente"])

    tab1, tab2 = st.tabs(["Tendencia", "Detalle de ventas"])

    with tab1:
        st.subheader("Tendencia diaria de ventas")

        tendencia = (
            dataframe_actual.groupby(dataframe_actual["Fecha"])["Toneladas"]
            .sum()
            .reset_index()
        )
        source_line_df = (
            alt.Chart(tendencia)
            .mark_line(point=True)
            .encode(
                x=alt.X("Fecha", title="Fecha"),
                y=alt.Y("Toneladas", title="Toneladas"),
                tooltip=["Fecha", "Toneladas"],
            )
        )

        st.altair_chart(source_line_df, use_container_width=True)

    with tab2:
        col1, col2 = st.columns([1, 1], gap="large")
        with col1:
            st.subheader("Ventas por producto")
            source_product_df = (
                alt.Chart(bar_df)
                .mark_bar()
                .encode(
                    x=alt.Y("Producto", sort=None, title="Producto"),
                    y=alt.Y("Toneladas", title="Toneladas"),
                    tooltip=["Producto", "Toneladas"],
                    color=alt.Color("Producto", legend=None),
                )
            )

            st.altair_chart(
                source_product_df,
                use_container_width=True,
            )

        with col2:
            st.subheader("Ventas por cliente")
            dataframe_clientes = (
                dataframe_actual.groupby(["Cliente", "Producto"])["Toneladas"]
                .sum()
                .reset_index()
                .sort_values(by="Toneladas", ascending=False)
            )
            level_columns = ["Producto", "Cliente"]
            value_column = "Toneladas"

            dataframe_tree = build_hierarchical_dataframe(
                dataframe_clientes, level_columns, value_column
            )

            st.write(dataframe_tree)

            fig = go.Figure()
            fig.add_trace(
                go.Sunburst(
                    labels=dataframe_tree["id"],
                    parents=dataframe_tree["parent"],
                    values=dataframe_tree["value"],
                    branchvalues="total",
                    marker=dict(
                        colorscale="RdBu",
                        cmid=0.5,
                    ),
                    hovertemplate="<b>%{label} </b> <br> Toneladas: %{value}<br> Padre: %{parent}",
                    name="",
                )
            )

            fig.update_layout(margin=dict(t=0, b=0, r=0, l=0))
            st.plotly_chart(fig, use_container_width=True)

        # DATAFRAME DETALLE DE VENTAS
        st.subheader("Detalle de ventas")
        st.dataframe(
            dataframe_actual,
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

    with st.sidebar:
        st.subheader("Filtros")
        input_data["year"] = st.number_input(
            "Año", min_value=2020, max_value=2024, step=1
        )
        input_data["month"] = st.slider("Mes", 1, 12)

        input_data["client"] = st.selectbox(
            "Cliente",
            get_clients(mysql)[0],
            index=None,
        )

        input_data["stage"] = st.radio(
            "Etapa",
            ["SEEDING", "VOLUMA"],
            index=None,
        )

    draw_results(input_data)


if __name__ == "__main__":
    main()
