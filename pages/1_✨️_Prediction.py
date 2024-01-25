"""
File Name: 1_Prediction.py

Author: Noelia Intriago (GitHub: NoeliaIntriago)

Creation date: 11/01/2024

Last modified: 21/01/2024

Description:
    This Python script is an integral part of a Streamlit dashboard application that focuses on 
    predicting shrimp feed production. It includes the `main` function for setting up the dashboard 
    layout, interactive elements for date selection and prediction, and functions for processing 
    predictions (`predict`) and generating visualizations (`build_week_charts`). The script also 
    contains functionalities to write the prediction results into an Excel file (`write_excel`) 
    for download.

Dependencies:
    - pandas
    - streamlit (st)
    - altair (for data visualization)
    - openpyxl (for Excel file handling)
    - io (for byte stream handling)

Functions:
    - main(): Sets up the Streamlit dashboard layout and interactivity.
    - predict(result): Forecasts future production volumes based on historical data.
    - build_week_dataframes(df): Processes data to create weekly pivot tables.
    - build_week_charts(df, num_week): Generates and displays weekly production charts.
    - write_excel(df1, df2, df3, df4, date): Creates an Excel file from weekly data.
    - process_display_data(date): Processes and displays prediction data for a given date.

Usage: 
    This script is executed to run the ShriMP Prediction Streamlit dashboard, providing an interactive 
    platform for predicting and analyzing shrimp feed production.
"""
from datetime import timedelta
from keras.models import load_model
from PIL import Image
from queries import get_clients, get_min_max_date, get_prediction_data
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler

import altair as alt
import io
import json
import mysql.connector
import os
import pandas as pd
import streamlit as st
import toml

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
model = load_model(path + "/../model/model_lstm_all_76_8.h5")

with open(path + "/../columns_data.json", "r") as f:
    data = json.load(f)

columns_out = data["columns_out"]
columns_order = data["columns_order"]
columns_promedio = data["columns_promedio"]


@st.cache_data
def predict(result):
    """
    Predicts the production of shrimp feed for the next 4 weeks using a pre-trained model.

    This function processes the input data 'result' which includes historical production data.
    It performs data resampling, flattening, and scaling to prepare the data for prediction.
    The prediction is made using a pre-trained machine learning model and then the output is
    inversely transformed to the original scale.

    The function handles specific data columns for input and output data, aggregation for
    resampling, and scaling for machine learning model prediction.

    Parameters:
        - result: A tuple where the first element is a DataFrame containing historical production data.

    Returns:
        - A numpy array containing the predicted production data for the next 4 weeks.

    Notes:
        - The function assumes specific column names and structure in the 'result' DataFrame.
        - It uses MinMaxScaler for scaling the input and output data.
        - The machine learning model used for prediction should be predefined as 'model'.
    """
    agregaciones = {
        col: "mean" if col in columns_promedio else "sum" for col in columns_order
    }

    # INPUT DATA -> BASE
    input_data = result[0][columns_order]
    input_data.index = pd.to_datetime(input_data.index)
    input_data = input_data.resample("W").agg(agregaciones).reset_index()
    input_data = input_data.tail(4)
    input_data = input_data.set_index("FECHA")

    input_data_flatten = (
        pd.concat([input_data.iloc[i] for i in range(input_data.shape[0])], axis=0)
        .to_frame()
        .T
    )
    week_number = input_data.index.isocalendar().week
    new_columns = [f"W{week}_{col}" for week in week_number for col in columns_order]
    input_data_flatten.columns = new_columns

    # OUTPUT DATA -> SOLO PRODUCTOS_CLIENTE
    output_data = result[0][columns_out]
    output_data.index = pd.to_datetime(output_data.index)
    output_data = output_data.resample("W").agg("sum").reset_index()
    output_data = output_data.tail(4)
    output_data = output_data.set_index("FECHA")

    output_data_flatten = (
        pd.concat([output_data.iloc[i] for i in range(output_data.shape[0])], axis=0)
        .to_frame()
        .T
    )
    week_number = output_data.index.isocalendar().week
    new_columns = [f"W{week}_{col}" for week in week_number for col in columns_out]
    output_data_flatten.columns = new_columns

    n_lags = 4
    # AQUÍ ESCALO
    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(input_data_flatten)
    n_features = int(
        X_scaled.shape[1] / n_lags
    )  # Número de características por paso de tiempo.
    X_reshaped = X_scaled.reshape((X_scaled.shape[0], n_lags, n_features))

    # AQUÍ PREDICE
    prediction = model.predict(X_reshaped)
    n_samples, n_weeks, n_features = prediction.shape

    # AQUÍ INVIERTO LA ESCALACIÓN
    scaler_out = MinMaxScaler()
    X_scaled_out = scaler_out.fit_transform(output_data_flatten)
    prediction_reshaped = prediction.reshape(n_samples, n_weeks * n_features)
    prediction = scaler_out.inverse_transform(prediction_reshaped)

    return prediction


def process_display_data(model_prediction, date):
    """
    Processes and displays shrimp feed production prediction data for a specified date.

    This function retrieves prediction data based on the provided date, applies the predictive model,
    and then processes and visualizes the predicted data for the next four weeks. It splits the model's
    prediction into weekly segments, creates dataframes for each week using `build_week_dataframes`,
    and displays charts for each week using `build_week_charts`.

    Parameters:
        - model_prediction: A numpy array containing the predicted production data for the next 4 weeks.
        - date: A datetime object representing the date for which the predictions are to be made.

    Returns:
        - A tuple of DataFrames for each of the four weeks containing the processed prediction data.

    Notes:
        - The function assumes a specific structure and naming convention of the columns in the prediction data.
        - Relies on `get_prediction_data`, `predict`, `build_week_dataframes`, and `build_week_charts` functions to fetch, predict, process, and visualize data respectively.
        - If no prediction data is available for the given date, the function will not perform any further actions.
    """
    dates = [
        date,
        date + timedelta(weeks=1),
        date + timedelta(weeks=2),
        date + timedelta(weeks=3),
    ]
    week1 = pd.DataFrame(model_prediction[0][0:70]).T
    week1.columns = columns_out

    week2 = pd.DataFrame(model_prediction[0][70:140]).T
    week2.columns = columns_out

    week3 = pd.DataFrame(model_prediction[0][140:210]).T
    week3.columns = columns_out

    week4 = pd.DataFrame(model_prediction[0][210:280]).T
    week4.columns = columns_out

    weekly_summary = build_weekly_summary(week1, week2, week3, week4, dates)
    build_charts(
        weekly_summary,
        "Predicción de producción de balanceado de camarón para las próximas 4 semanas",
    )

    week1 = build_week_dataframes(week1)
    week2 = build_week_dataframes(week2)
    week3 = build_week_dataframes(week3)
    week4 = build_week_dataframes(week4)

    seeding_weekly_df = build_line_group_dataframes(
        week1, week2, week3, week4, dates, "SEEDING"
    )
    voluma_weekly_df = build_line_group_dataframes(
        week1, week2, week3, week4, dates, "VOLUMA"
    )

    st.divider()
    col1, col2 = st.columns([1, 1], gap="large")
    with col1:
        build_charts(
            seeding_weekly_df,
            "Predicción de producción de balanceado de camarón para el grupo de líneas SEEDING",
        )

    with col2:
        build_charts(
            voluma_weekly_df,
            "Predicción de producción de balanceado de camarón para el grupo de líneas VOLUMA",
        )

    return week1, week2, week3, week4


@st.cache_data
def build_weekly_summary(w1, w2, w3, w4, dates):
    """
    Builds a global DataFrame with the predicted production data for the next four weeks.

    This function takes the four DataFrames for each week and concatenates them into a single DataFrame.
    It also adds a column for the week number to the DataFrame.

    Parameters:
        - w1, w2, w3, w4: DataFrames for weeks 1, 2, 3, and 4, respectively.

    Returns:
        - A DataFrame containing the predicted production data for the next four weeks.

    Notes:
        - Assumes a specific format for the input DataFrames.
    """
    total_week1 = w1.sum(axis=1).values[0]
    total_week2 = w2.sum(axis=1).values[0]
    total_week3 = w3.sum(axis=1).values[0]
    total_week4 = w4.sum(axis=1).values[0]

    df = pd.DataFrame(
        {
            "Semana": [1, 2, 3, 4],
            "Fecha": dates,
            "Total": [total_week1, total_week2, total_week3, total_week4],
        }
    )

    return df


@st.cache_data
def build_line_group_dataframes(w1, w2, w3, w4, dates, line_group):
    """
    Builds a DataFrame containing the total production for a given product-stage.

    This function takes the four DataFrames for each week and concatenates them into a single DataFrame.
    It also adds a column for the week number to the DataFrame.

    Parameters:
        - w1, w2, w3, w4: DataFrames for weeks 1, 2, 3, and 4, respectively.
        - dates: A list of dates for each week.
        - line_group: A string representing the product-stage for which the total production is to be calculated.

    Returns:
        - A DataFrame containing the total production for the given product-stage for each week.

    Notes:
        - Assumes a specific format for the input DataFrames.
    """
    week_dataframes = [w1, w2, w3, w4]
    week_totals = []

    for week_df, date in zip(week_dataframes, dates):
        group_columns = [col for col in week_df.columns if line_group in col]
        week_total = week_df[group_columns].sum(axis=1).iloc[0]
        week_totals.append([date, week_total])

    df = pd.DataFrame(week_totals, columns=["Fecha", "Total"])
    df["Semana"] = range(1, len(week_dataframes) + 1)

    return df


@st.cache_data
def build_week_dataframes(df):
    """
    Builds a pivot DataFrame for each week from the provided DataFrame.

    This function melts the original DataFrame to long format and then creates a pivot table. It extracts
    client and product-stage information from the column names and maps client IDs to their names.

    Parameters:
        - df: DataFrame containing weekly data with specific column naming conventions (e.g., PRODUCT_STAGE_WeekNumber).

    Returns:
        - A pivot DataFrame with 'CLIENTE' as rows, 'PRODUCTO_ETAPA' as columns, and their corresponding values.

    Notes:
        - Assumes a specific format for the input DataFrame's column names.
        - Requires a predefined function `get_clients` to map client IDs to names.
    """
    clientes = get_clients(mysql)[0]
    mapeo_clientes = {i + 1: nombre for i, nombre in enumerate(clientes)}

    df_melt = df.melt(var_name="columna", value_name="valor")
    df_melt["CLIENTE"] = df_melt["columna"].str.extract("(\d+)$").astype(int)
    df_melt["PRODUCTO_ETAPA"] = df_melt["columna"].str.extract("^(.*?)_\d+$")

    df_pivot = df_melt.pivot_table(
        index="CLIENTE", columns="PRODUCTO_ETAPA", values="valor", aggfunc="first"
    )
    df_pivot = df_pivot.reset_index()
    df_pivot.columns.name = None
    df_pivot["CLIENTE"] = df_pivot["CLIENTE"].replace(mapeo_clientes)

    return df_pivot


def build_charts(df, title):
    """
    Builds a line chart for the provided DataFrame.

    This function takes a DataFrame and generates a line chart using the 'Fecha' column as the x-axis
    and the 'Total' column as the y-axis. It also sets the x-axis labels to the 'Fecha' column values.

    Parameters:
        - df: DataFrame containing the data to be plotted.
        - title: A string representing the title of the chart.

    Side Effects:
        - Displays an interactive line chart on the Streamlit dashboard.
    """
    chart_color = "#F84718"
    line_opacity = 0.8
    line_size = 3
    point_size = 50

    df["Fecha"] = df["Fecha"].astype(str)
    x_axis_labels = df["Fecha"].tolist()

    st.altair_chart(
        alt.Chart(df)
        .mark_line(color=chart_color, opacity=line_opacity, size=line_size, filled=True)
        .encode(
            x=alt.X(
                "Fecha",
                sort=None,
                title="Semana",
                axis=alt.Axis(values=x_axis_labels, labelAngle=-45),
            ),
            y=alt.Y("Total:Q", title="Toneladas"),
        )
        .properties(title=title)
        + alt.Chart(df)
        .mark_point(
            color=chart_color,
            size=point_size,
            filled=True,
        )
        .encode(x="Fecha", y="Total:Q"),
        use_container_width=True,
    )


def write_excel(df1, df2, df3, df4, date):
    """
    Writes multiple DataFrames to an Excel file and returns the file as a byte stream.

    The function takes four DataFrames corresponding to four weeks of data and a start date. It writes
    each DataFrame each DataFrame in an Excel file separated by week.

    Parameters:
        - df1, df2, df3, df4: DataFrames for weeks 1, 2, 3, and 4, respectively.
        - date: A datetime object representing the start date for the first week.

    Returns:
        - A byte stream of the generated Excel file.

    Dependencies:
        - pandas
        - openpyxl
    """
    output = io.BytesIO()

    df1.insert(0, "Semana", f"Semana 1 ({date.strftime('%Y-%m-%d')})")
    df2.insert(
        0, "Semana", f"Semana 2 ({(date + timedelta(weeks=1)).strftime('%Y-%m-%d')})"
    )
    df3.insert(
        0, "Semana", f"Semana 3 ({(date + timedelta(weeks=2)).strftime('%Y-%m-%d')})"
    )
    df4.insert(
        0, "Semana", f"Semana 4 ({(date + timedelta(weeks=3)).strftime('%Y-%m-%d')})"
    )

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name="Semanas", startrow=0, index=False)

        startrow = len(df1) + 2

        for i, df in enumerate([df2, df3, df4], start=1):
            df.to_excel(
                writer,
                sheet_name="Semanas",
                startrow=startrow,
                header=False,
                index=False,
            )
            startrow += len(df) + 1

    processed_data = output.getvalue()
    return processed_data


def main():
    """
    Main function for the ShriMP Prediction Streamlit dashboard.

    This function sets up the Streamlit page configuration and styles, displays a logo, and explains
    the functionality of the dashboard. It includes a date input form for the user to select a date
    for shrimp feed production prediction. Upon form submission, it processes the selected date,
    displays the predicted production for the next four weeks, and provides an option to download
    this data as an Excel report.

    The function integrates various components like database querying for date range, data processing
    for predictions, and data visualization.

    Side Effects:
        - Sets the Streamlit page configuration and renders HTML/CSS for styling.
        - Interacts with global session state for storing user input.
        - Calls functions for data processing and Excel report generation.
        - Displays interactive elements, text, and images on the Streamlit dashboard.
    """
    st.set_page_config(
        page_title="ShriMP Prediction", layout="wide", page_icon=":shrimp:"
    )

    with open(path + "/../style/prediction_style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    with open(path + "/../style/style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    logo = Image.open(path + "/../images/logo_ai.png")
    col1, col2 = st.columns([0.5, 2])
    col1.image(logo, width=175)
    col2.title("ShriMP Prediction")

    min_date = get_min_max_date(mysql)[0][0] + timedelta(weeks=4)
    max_date = get_min_max_date(mysql)[0][1]

    st.subheader("¿Cómo funciona?")
    st.write(
        """
        ShriMP Prediction se conecta a una base de datos de Expocam para obtener la información de ventas. Esta información es procesada y visualizada en gráficos y tablas.
        La predicción de producción de balanceado de camarón se realiza utilizando un modelo LSTM entrenado con datos históricos de producción de balanceado de camarón.
        La predicción mostrará la producción de balanceado de camarón para las siguientes 4 semanas a partir de la fecha seleccionada.
    """
    )

    st.divider()

    col1, col2 = st.columns([1, 1])
    col1.subheader("Predicción de producción de balanceado de camarón")
    col1.write(
        """
        Para realizar la predicción de producción de balanceado de camarón, seleccione una fecha y haga clic en el botón "Predecir".
    """
    )

    form = col2.form(key="prediction_form")
    with form:
        date = st.date_input(
            "Fecha",
            value=None,
            min_value=min_date,
            max_value=max_date,
            format="YYYY-MM-DD",
        )
        predict_button = st.form_submit_button(label="Predecir")

        if predict_button:
            st.session_state.predicted_date = date

    if "predicted_date" in st.session_state and date is not None:
        prediction_input = get_prediction_data(_connection=mysql, date=date)
        model_prediction = predict(prediction_input)
        week1, week2, week3, week4 = process_display_data(model_prediction, date)

        excel = write_excel(week1, week2, week3, week4, date)
        col2.download_button(
            "Descargar Reporte Excel",
            data=excel,
            file_name=f"reporte_prediccion_{date}.xlsx",
            mime="application/vnd.ms-excel",
        )


if __name__ == "__main__":
    st.cache_data.clear()
    main()
