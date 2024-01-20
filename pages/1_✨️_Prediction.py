from datetime import timedelta
from keras.models import load_model
from PIL import Image
from queries import get_clients, get_min_max_date, get_prediction_data
from sklearn.preprocessing import MinMaxScaler

import altair as alt
import io
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


def predict(result):
    columns_order = [
        "AUR_SEEDING_1",
        "AUR_VOLUMA_1",
        "TAR_SEEDING_1",
        "JIN_SEEDING_1",
        "ZEP_VOLUMA_1",
        "JIN_VOLUMA_1",
        "VEX_VOLUMA_1",
        "NYX_SEEDING_1",
        "NYX_VOLUMA_1",
        "LEX_VOLUMA_1",
        "NICOVITA_1",
        "POTENCIAL_GRUPO_1",
        "SOW_MAX_ALCANZABLE_1",
        "AUR_SEEDING_2",
        "AUR_VOLUMA_2",
        "TAR_SEEDING_2",
        "JIN_SEEDING_2",
        "ZEP_VOLUMA_2",
        "JIN_VOLUMA_2",
        "VEX_VOLUMA_2",
        "NYX_SEEDING_2",
        "NYX_VOLUMA_2",
        "LEX_VOLUMA_2",
        "NICOVITA_2",
        "POTENCIAL_GRUPO_2",
        "SOW_MAX_ALCANZABLE_2",
        "AUR_SEEDING_3",
        "AUR_VOLUMA_3",
        "TAR_SEEDING_3",
        "JIN_SEEDING_3",
        "ZEP_VOLUMA_3",
        "JIN_VOLUMA_3",
        "VEX_VOLUMA_3",
        "NYX_SEEDING_3",
        "NYX_VOLUMA_3",
        "LEX_VOLUMA_3",
        "NICOVITA_3",
        "POTENCIAL_GRUPO_3",
        "SOW_MAX_ALCANZABLE_3",
        "AUR_SEEDING_4",
        "AUR_VOLUMA_4",
        "TAR_SEEDING_4",
        "JIN_SEEDING_4",
        "ZEP_VOLUMA_4",
        "JIN_VOLUMA_4",
        "VEX_VOLUMA_4",
        "NYX_SEEDING_4",
        "NYX_VOLUMA_4",
        "LEX_VOLUMA_4",
        "NICOVITA_4",
        "POTENCIAL_GRUPO_4",
        "SOW_MAX_ALCANZABLE_4",
        "AUR_SEEDING_5",
        "AUR_VOLUMA_5",
        "TAR_SEEDING_5",
        "JIN_SEEDING_5",
        "ZEP_VOLUMA_5",
        "JIN_VOLUMA_5",
        "VEX_VOLUMA_5",
        "NYX_SEEDING_5",
        "NYX_VOLUMA_5",
        "LEX_VOLUMA_5",
        "NICOVITA_5",
        "POTENCIAL_GRUPO_5",
        "SOW_MAX_ALCANZABLE_5",
        "AUR_SEEDING_6",
        "AUR_VOLUMA_6",
        "TAR_SEEDING_6",
        "JIN_SEEDING_6",
        "ZEP_VOLUMA_6",
        "JIN_VOLUMA_6",
        "VEX_VOLUMA_6",
        "NYX_SEEDING_6",
        "NYX_VOLUMA_6",
        "LEX_VOLUMA_6",
        "NICOVITA_6",
        "POTENCIAL_GRUPO_6",
        "SOW_MAX_ALCANZABLE_6",
        "AUR_SEEDING_7",
        "AUR_VOLUMA_7",
        "TAR_SEEDING_7",
        "JIN_SEEDING_7",
        "ZEP_VOLUMA_7",
        "JIN_VOLUMA_7",
        "VEX_VOLUMA_7",
        "NYX_SEEDING_7",
        "NYX_VOLUMA_7",
        "LEX_VOLUMA_7",
        "NICOVITA_7",
        "POTENCIAL_GRUPO_7",
        "SOW_MAX_ALCANZABLE_7",
        "TOTAL_LB",
        "TOTAL_FOB",
        "30-40 (29 g)",
        "40-50 (23 g)",
        "50-60 (18 g)",
        "60-70 (15 g)",
        "70-80 (13 g)",
        "80-100 (11 g)",
        "TOTAL_USD_LECITINA",
        "LIBRAS_NETO_LECITINA",
        "TOTAL_USD_SOYA",
        "LIBRAS_NETO_SOYA",
        "TOTAL_USD_TRIGO",
        "LIBRAS_NETO_TRIGO",
    ]

    columns_out = [
        "AUR_SEEDING_1",
        "AUR_VOLUMA_1",
        "TAR_SEEDING_1",
        "JIN_SEEDING_1",
        "ZEP_VOLUMA_1",
        "JIN_VOLUMA_1",
        "VEX_VOLUMA_1",
        "NYX_SEEDING_1",
        "NYX_VOLUMA_1",
        "LEX_VOLUMA_1",
        "AUR_SEEDING_2",
        "AUR_VOLUMA_2",
        "TAR_SEEDING_2",
        "JIN_SEEDING_2",
        "ZEP_VOLUMA_2",
        "JIN_VOLUMA_2",
        "VEX_VOLUMA_2",
        "NYX_SEEDING_2",
        "NYX_VOLUMA_2",
        "LEX_VOLUMA_2",
        "AUR_SEEDING_3",
        "AUR_VOLUMA_3",
        "TAR_SEEDING_3",
        "JIN_SEEDING_3",
        "ZEP_VOLUMA_3",
        "JIN_VOLUMA_3",
        "VEX_VOLUMA_3",
        "NYX_SEEDING_3",
        "NYX_VOLUMA_3",
        "LEX_VOLUMA_3",
        "AUR_SEEDING_4",
        "AUR_VOLUMA_4",
        "TAR_SEEDING_4",
        "JIN_SEEDING_4",
        "ZEP_VOLUMA_4",
        "JIN_VOLUMA_4",
        "VEX_VOLUMA_4",
        "NYX_SEEDING_4",
        "NYX_VOLUMA_4",
        "LEX_VOLUMA_4",
        "AUR_SEEDING_5",
        "AUR_VOLUMA_5",
        "TAR_SEEDING_5",
        "JIN_SEEDING_5",
        "ZEP_VOLUMA_5",
        "JIN_VOLUMA_5",
        "VEX_VOLUMA_5",
        "NYX_SEEDING_5",
        "NYX_VOLUMA_5",
        "LEX_VOLUMA_5",
        "AUR_SEEDING_6",
        "AUR_VOLUMA_6",
        "TAR_SEEDING_6",
        "JIN_SEEDING_6",
        "ZEP_VOLUMA_6",
        "JIN_VOLUMA_6",
        "VEX_VOLUMA_6",
        "NYX_SEEDING_6",
        "NYX_VOLUMA_6",
        "LEX_VOLUMA_6",
        "AUR_SEEDING_7",
        "AUR_VOLUMA_7",
        "TAR_SEEDING_7",
        "JIN_SEEDING_7",
        "ZEP_VOLUMA_7",
        "JIN_VOLUMA_7",
        "VEX_VOLUMA_7",
        "NYX_SEEDING_7",
        "NYX_VOLUMA_7",
        "LEX_VOLUMA_7",
    ]

    columns_promedio = [
        "30-40 (29 g)",
        "40-50 (23 g)",
        "50-60 (18 g)",
        "60-70 (15 g)",
        "70-80 (13 g)",
        "80-100 (11 g)",
        "NICOVITA_1",
        "POTENCIAL_GRUPO_1",
        "SOW_MAX_ALCANZABLE_1",
        "NICOVITA_2",
        "POTENCIAL_GRUPO_2",
        "SOW_MAX_ALCANZABLE_2",
        "NICOVITA_3",
        "POTENCIAL_GRUPO_3",
        "SOW_MAX_ALCANZABLE_3",
        "NICOVITA_4",
        "POTENCIAL_GRUPO_4",
        "SOW_MAX_ALCANZABLE_4",
        "NICOVITA_5",
        "POTENCIAL_GRUPO_5",
        "SOW_MAX_ALCANZABLE_5",
        "NICOVITA_6",
        "POTENCIAL_GRUPO_6",
        "SOW_MAX_ALCANZABLE_6",
        "NICOVITA_7",
        "POTENCIAL_GRUPO_7",
        "SOW_MAX_ALCANZABLE_7",
    ]
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

    return prediction, columns_out


def build_week_dataframes(df):
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


def build_week_charts(df, num_week):
    df_pivot = df.melt(
        id_vars=["CLIENTE"], var_name="PRODUCTO_ETAPA", value_name="VALOR"
    )

    st.altair_chart(
        alt.Chart(df_pivot)
        .mark_line(point=True)
        .encode(
            x=alt.X("CLIENTE:N", sort=None, title="Cliente"),
            y=alt.Y("VALOR:Q", title="Toneladas"),
            color=alt.Color("PRODUCTO_ETAPA:N", legend=alt.Legend(title="Producto")),
        )
        .properties(
            title=f"Predicción de producción de balanceado de camarón para la semana {num_week}"
        ),
        use_container_width=True,
    )


def write_excel(df1, df2, df3, df4, date):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name=f"Semana 1 ({date})")
        df2.to_excel(writer, sheet_name=f"Semana 2 ({date + timedelta(weeks=1)})")
        df3.to_excel(writer, sheet_name=f"Semana 3 ({date + timedelta(weeks=2)})")
        df4.to_excel(writer, sheet_name=f"Semana 4 ({date + timedelta(weeks=3)})")

    processed_data = output.getvalue()
    return processed_data


def main():
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
            key=None,
            help=None,
            format="YYYY-MM-DD",
        )
        predict_button = st.form_submit_button(label="Predecir")
        prediction_data = None

        if predict_button and date is not None:
            prediction_data = get_prediction_data(mysql, date)

        elif predict_button and date is None:
            st.write("Por favor seleccione una fecha.")

    if prediction_data is not None:
        model_prediction, columns_out = predict(prediction_data)

        week1 = pd.DataFrame(model_prediction[0][0:70]).T
        week1.columns = columns_out
        week1 = build_week_dataframes(week1)
        build_week_charts(week1, 1)

        week2 = pd.DataFrame(model_prediction[0][70:140]).T
        week2.columns = columns_out
        week2 = build_week_dataframes(week2)
        build_week_charts(week2, 2)

        week3 = pd.DataFrame(model_prediction[0][140:210]).T
        week3.columns = columns_out
        week3 = build_week_dataframes(week3)
        build_week_charts(week3, 3)

        week4 = pd.DataFrame(model_prediction[0][210:280]).T
        week4.columns = columns_out
        week4 = build_week_dataframes(week4)
        build_week_charts(week4, 4)

        excel = write_excel(week1, week2, week3, week4, date)
        col2.download_button(
            "Descargar Reporte Excel",
            data=excel,
            file_name=f"reporte_prediccion_{date}.xlsx",
            mime="application/vnd.ms-excel",
        )


if __name__ == "__main__":
    main()
