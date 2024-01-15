from keras.models import load_model
from PIL import Image
from queries import predict

import altair as alt
import mysql.connector
import os
import streamlit as st
import toml

path = os.path.dirname(__file__)
toml_data = toml.load(path + '/../.streamlit/secrets.toml')

HOST = toml_data['mysql']['host']
DATABASE = toml_data['mysql']['database']
USER = toml_data['mysql']['user']
PASSWORD = toml_data['mysql']['password']
PORT = toml_data['mysql']['port']

mysql = mysql.connector.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)
model = load_model(path + '/../model/model_lstm_all_76_8.h5')

def main():
    st.set_page_config(page_title='ShriMP Prediction', layout="wide", page_icon=':shrimp:')

    with open(path+"/../style/prediction_style.css") as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

    logo = Image.open(path +"/../images/logo_ai.png")
    col1, col2 = st.columns([0.5, 2])
    col1.image(logo, width=175)
    col2.title('ShriMP Prediction')

    st.subheader('¿Cómo funciona?')
    st.write('''
        ShriMP Prediction se conecta a una base de datos de Expocam para obtener la información de ventas. Esta información es procesada y visualizada en gráficos y tablas.
        La predicción de producción de balanceado de camarón se realiza utilizando un modelo LSTM entrenado con datos históricos de producción de balanceado de camarón.
        La predicción mostrará la producción de balanceado de camarón para las siguientes 4 semanas a partir de la fecha seleccionada.
    ''')

    st.divider()

    st.subheader('Predicción de producción de balanceado de camarón')

    col1, col2 = st.columns([1, 1])
    col1.write('''
        Para realizar la predicción de producción de balanceado de camarón, seleccione una fecha y haga clic en el botón "Predecir".
    ''')

    date = col2.date_input('Fecha', value=None, min_value=None, max_value=None, key=None, help=None)
    if col2.button('Predecir', type="primary"):
        prediction = predict(mysql, model, date)
        st.write(f'La predicción de producción de balanceado de camarón para el día {date} es de {prediction} toneladas.')

if __name__ == "__main__":
    main()