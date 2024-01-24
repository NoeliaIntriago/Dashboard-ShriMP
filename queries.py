"""
File Name: queries.py

Author: Noelia Intriago (GitHub: NoeliaIntriago)

Creation date: 11/01/2024

Last modified: 22/01/2024

Description: This script contains a collection of functions used for executing various SQL queries to a MySQL database. 
    The functions in this file are primarily used to retrieve data for different aspects of the ShriMP 
    dashboard application. This includes fetching data related to clients, historic transactions, price 
    information, and other relevant data sets required by the application.

    Each function within this file is designed to connect to the database, execute a specific SQL query, 
    handle any exceptions, and return the results or an error message. The functions are typically used 
    to support the data fetching requirements of different components of the dashboard.

Dependencies:
    - pandas (for data handling)
    - streamlit (st) (for Streamlit application components)
    - mysql.connector (for MySQL database interactions)
    - traceback (for error handling)

Functions:
    - get_min_max_date(connection): Retrieves minimum and maximum date from a specific table.
    - get_clients(connection): Fetches client data from the database.
    - get_historic(connection, params): Retrieves historical data based on given parameters.
    - get_prediction_data(_connection, date): Retrieves data for prediction based on given date.
    - check_previous_month_data(connection, date, table): Checks if previous month data exists.
    - check_already_uploaded_data(connection, date, table): Checks if data has already been uploaded.
    - upload_ventas_data(connection, data): Uploads ventas data to the database.
    - upload_materia_prima_data(connection, data): Uploads materia prima data to the database.
    - upload_precio_camaron_data(connection, data): Uploads precio camaron data to the database.
    - upload_sow_data(connection, data): Uploads sow data to the database.
    - upload_exportaciones_data(connection, data): Uploads exportaciones data to the database.

Usage: 
    These functions are called by various components of the Streamlit dashboard application to 
    retrieve necessary data from the MySQL database, enabling dynamic data-driven functionality 
    in the application.
"""
from datetime import datetime, timedelta

import calendar as cal
import pandas as pd
import streamlit as st
import traceback


## GET MIN_DATE AND MAX_DATE
def get_min_max_date(connection):
    """
    Retrieves the minimum and maximum dates from the 'venta' table in the database.

    This function executes a SQL query to find the earliest and latest dates of sale transactions.
    It is used to determine the range of data available for analysis or display in the application.

    Parameters:
        - connection: The database connection object.

    Returns:
        - On success, returns a tuple containing the minimum and maximum dates, and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT MIN(fecha_emision) as MIN_DATE, MAX(fecha_emision) as MAX_DATE FROM venta"
        )
        data = cursor.fetchone()
        cursor.close()

        return data, 200
    except Exception as e:
        print(traceback.format_exc())
        return "Something went wrong", 500


## GET ALL CLIENTS
def get_clients(connection):
    """
    Retrieves all clients from the 'cliente' table in the database.

    This function executes a SQL query to fetch all clients from the database. It is used to populate
    the client selection dropdown in the application.

    Parameters:
        - connection: The database connection object.

    Returns:
        - On success, returns a tuple containing the list of clients and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM cliente")
        data = [row[2] for row in cursor.fetchall()]
        cursor.close()

        return data, 200
    except Exception as e:
        print(traceback.format_exc())
        return "Something went wrong", 500


## GET HISTORY
def get_historic(connection, params):
    """
    Retrieves historic data based on given parameters.

    This function executes a SQL query to fetch historic data from the database. It is used to populate
    the detailed sales data table.

    Parameters:
        - connection: The database connection object.
        - params: A data structure containing the necessary parameters for the sales data query. The exact structure is dependent on the database schema and query requirements.

    Returns:
        - On success, returns a tuple containing the list of historic sales data and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    try:
        year = int(params["year"])
        month = int(params["month"])

        current_period = datetime(year, month, 1)
        current_start_date = current_period.strftime("%Y-%m-%d")
        current_end_date = datetime(
            year, month, cal.monthrange(year, month)[1]
        ).strftime("%Y-%m-%d")

        past_period = (current_period - timedelta(days=1)).replace(day=1)
        past_start_date = past_period.strftime("%Y-%m-%d")
        past_end_date = datetime(
            past_period.year,
            past_period.month,
            cal.monthrange(past_period.year, past_period.month)[1],
        ).strftime("%Y-%m-%d")

        current_period_query = f"""
            SELECT 
                des_cliente as Cliente,
                des_sku as Producto,
                familia as Familia,
                fecha_emision as Fecha,
                grupo_linea as Etapa,
                toneladas as Toneladas
            FROM venta 
            INNER JOIN cliente ON venta.id_cliente = cliente.id_cliente 
            INNER JOIN producto ON venta.id_producto = producto.id_producto
            WHERE venta.fecha_emision BETWEEN '{current_start_date}' AND '{current_end_date}'
        """

        past_period_query = f"""
            SELECT 
                des_cliente as Cliente,
                des_sku as Producto,
                familia as Familia,
                fecha_emision as Fecha,
                grupo_linea as Etapa,
                toneladas as Toneladas
            FROM venta 
            INNER JOIN cliente ON venta.id_cliente = cliente.id_cliente 
            INNER JOIN producto ON venta.id_producto = producto.id_producto
            WHERE venta.fecha_emision BETWEEN '{past_start_date}' AND '{past_end_date}'
        """

        conditions = []

        if params["stage"] is not None:
            conditions.append(f"producto.grupo_linea = '{params['stage']}'")

        if params["client"] is not None:
            conditions.append(f"cliente.des_cliente = '{params['client']}'")

        final_current_query = f"{current_period_query} {' AND '.join(conditions)}"
        final_past_query = f"{past_period_query} {' AND '.join(conditions)}"

        cursor = connection.cursor()
        cursor.execute(final_current_query)
        data_current = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        cursor.close()
        formatted_current = [
            {columns[i]: row[i] for i in range(len(columns))} for row in data_current
        ]

        cursor = connection.cursor()
        cursor.execute(final_past_query)
        data_past = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        cursor.close()
        formatted_past = [
            {columns[i]: row[i] for i in range(len(columns))} for row in data_past
        ]

        return formatted_current, formatted_past, 200
    except Exception as e:
        print(traceback.format_exc())
        return "Something went wrong", 500


@st.cache_data
## GET PREDICTION
def get_prediction_data(_connection, date):
    """
    Retrieves data for prediction based on given date.

    This function executes a series of SQL queries to fetch data from the database. It is used to
    generate the data required for the production prediction model.

    Parameters:
        - _connection: The database connection object.
        - date: The date for which the prediction data is required.

    Returns:
        - On success, returns a tuple containing the prediction data and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    fecha_desde = date - timedelta(weeks=4)
    fecha_hasta = date - timedelta(days=1)

    try:
        dataframe_mp = get_materia_prima_for_prediction(
            _connection.cursor(), fecha_desde, fecha_hasta
        )
        dataframe_sow = get_sow_for_prediction(
            _connection.cursor(), fecha_desde, fecha_hasta
        )
        dataframe_precios_camaron = get_precios_camaron_for_prediction(
            _connection.cursor(), fecha_desde, fecha_hasta
        )
        dataframe_exportaciones = get_exportaciones_for_prediction(
            _connection.cursor(), fecha_desde, fecha_hasta
        )
        dataframe_ventas = get_ventas_for_prediction(
            _connection.cursor(), fecha_desde, fecha_hasta
        )

        merge_ventas_sow = pd.merge(
            dataframe_ventas, dataframe_sow, on="MES", how="left"
        )
        merge_ventas_sow = merge_ventas_sow.drop(["MES"], axis=1)

        merge_ventas_sow["FECHA"] = pd.to_datetime(
            merge_ventas_sow["FECHA"]
        ).dt.strftime("%Y-%m-%d")
        dataframe_exportaciones["FECHA"] = pd.to_datetime(
            dataframe_exportaciones["FECHA"]
        ).dt.strftime("%Y-%m-%d")
        dataframe_mp["FECHA"] = pd.to_datetime(dataframe_mp["FECHA"]).dt.strftime(
            "%Y-%m-%d"
        )
        dataframe_precios_camaron["FECHA"] = pd.to_datetime(
            dataframe_precios_camaron["FECHA"]
        ).dt.strftime("%Y-%m-%d")

        merged_data = pd.merge(
            merge_ventas_sow, dataframe_exportaciones, on="FECHA", how="left"
        )
        merged_data = pd.merge(merged_data, dataframe_mp, on="FECHA", how="left")
        merged_data = pd.merge(
            merged_data, dataframe_precios_camaron, on="FECHA", how="left"
        )

        merged_data = merged_data.set_index("FECHA")
        merged_data = merged_data.fillna(0)

        return merged_data, 200

    except Exception as e:
        print(traceback.format_exc())
        return "Something went wrong", 500


## GET MATERIA PRIMA INFORMATION FOR PREDICTION
def get_materia_prima_for_prediction(cursor, fecha_desde, fecha_hasta):
    """
    Retrieves materia prima data based on given date range.

    This function executes a SQL query to fetch materia prima data from the database. It is used to
    generate the data required for the production prediction model.

    Parameters:
        - cursor: The database cursor object.
        - fecha_desde: The start date of the date range.
        - fecha_hasta: The end date of the date range.

    Returns:
        - On success, returns a dataframe containing the materia prima data and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    cursor.execute(
        f"""
        SELECT 
            fecha,
            total_usd_lecitina,
            libras_neto_lecitina,
            total_usd_soya,
            libras_neto_soya,
            total_usd_trigo,
            libras_neto_trigo
        FROM materia_prima
        WHERE fecha BETWEEN '{fecha_desde}' AND '{fecha_hasta}'
    """
    )
    data_mp = cursor.fetchall()
    columns_mp = [i[0] for i in cursor.description]
    dataframe_mp = pd.DataFrame(data_mp, columns=columns_mp)
    dataframe_mp.columns = [
        "FECHA",
        "TOTAL_USD_LECITINA",
        "LIBRAS_NETO_LECITINA",
        "TOTAL_USD_SOYA",
        "LIBRAS_NETO_SOYA",
        "TOTAL_USD_TRIGO",
        "LIBRAS_NETO_TRIGO",
    ]

    dataframe_mp["LIBRAS_NETO_LECITINA"] = (
        dataframe_mp["LIBRAS_NETO_LECITINA"].astype(float)
    ) / 2000
    dataframe_mp["LIBRAS_NETO_SOYA"] = (
        dataframe_mp["LIBRAS_NETO_SOYA"].astype(float)
    ) / 2000
    dataframe_mp["LIBRAS_NETO_TRIGO"] = (
        dataframe_mp["LIBRAS_NETO_TRIGO"].astype(float)
    ) / 2000

    dataframe_mp["TOTAL_USD_LECITINA"] = (
        dataframe_mp["TOTAL_USD_LECITINA"].astype(float)
    ) * 2000
    dataframe_mp["TOTAL_USD_SOYA"] = (
        dataframe_mp["TOTAL_USD_SOYA"].astype(float)
    ) * 2000
    dataframe_mp["TOTAL_USD_TRIGO"] = (
        dataframe_mp["TOTAL_USD_TRIGO"].astype(float)
    ) * 2000

    return dataframe_mp


## GET SOW INFORMATION FOR PREDICTION
def get_sow_for_prediction(cursor, fecha_desde, fecha_hasta):
    """
    Retrieves sow data based on given date range.

    This function executes a SQL query to fetch sow data from the database. It is used to
    generate the data required for the production prediction model.

    Parameters:
        - cursor: The database cursor object.
        - fecha_desde: The start date of the date range.
        - fecha_hasta: The end date of the date range.

    Returns:
        - On success, returns a dataframe containing the sow data and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    columnas = ["NICOVITA", "POTENCIAL_GRUPO", "SOW_MAX_ALCANZABLE"]
    clientes = range(1, 8)

    columnas_esperadas = [
        f"{column}_{cliente}" for cliente in clientes for column in columnas
    ]
    columnas_esperadas.insert(0, "FECHA")
    columnas_esperadas.insert(1, "COD_CLIENTE")
    dataframe_esperado = pd.DataFrame(columns=columnas_esperadas)

    # CONSTRUCCIÓN DE DATAFRAME CON DATA DE SOW
    cursor.execute(
        f"""
        SELECT 
            fecha_periodo as fecha,
            cod_cliente,
            potencial_grupo,
            nicovita,
            sow_max_alcanzable
        FROM sow
        INNER JOIN cliente ON sow.id_cliente = cliente.id_cliente 
        WHERE
            (YEAR(fecha_periodo) = YEAR('{fecha_desde}') AND MONTH(fecha_periodo) >= MONTH('{fecha_desde}'))
            AND
            (YEAR(fecha_periodo) = YEAR('{fecha_hasta}') AND MONTH(fecha_periodo) <= MONTH('{fecha_hasta}'))
    """
    )
    data_sow = cursor.fetchall()
    columns_sow = [i[0] for i in cursor.description]
    dataframe_sow = pd.DataFrame(data_sow, columns=columns_sow)
    dataframe_sow.columns = [column.upper() for column in dataframe_sow.columns.values]
    dataframe_sow["MES"] = pd.to_datetime(dataframe_sow["FECHA"]).dt.month

    dataframe_sow = (
        dataframe_sow.groupby(["FECHA", "COD_CLIENTE"])
        .agg(
            {"POTENCIAL_GRUPO": "sum", "NICOVITA": "mean", "SOW_MAX_ALCANZABLE": "mean"}
        )
        .reset_index()
    )
    dataframe_sow = dataframe_sow.pivot_table(
        index=["FECHA"],
        columns="COD_CLIENTE",
        values=["POTENCIAL_GRUPO", "NICOVITA", "SOW_MAX_ALCANZABLE"],
        aggfunc="first",
    ).fillna(0)
    new_columns = {}
    for column in dataframe_sow.columns:
        if "NICOVITA" in column:
            new_columns[column] = f"NICOVITA_{column[-1]}"
        elif "POTENCIAL_GRUPO" in column:
            new_columns[column] = f"POTENCIAL_GRUPO_{column[-1]}"
        elif "SOW_MAX_ALCANZABLE" in column:
            new_columns[column] = f"SOW_MAX_ALCANZABLE_{column[-1]}"

    dataframe_sow = dataframe_sow.rename(columns=new_columns)
    dataframe_sow.columns = [
        f"{column[0]}_{column[1][-1]}" for column in dataframe_sow.columns.values
    ]
    dataframe_sow = dataframe_sow.reset_index()

    # LÓGICA PARA LLENAR DATAFRAME VACÍO CON DATA DE SOW
    dataframe_sow = dataframe_sow.reindex(columns=columnas_esperadas, fill_value=0)
    df_resultado = pd.concat([dataframe_esperado, dataframe_sow])
    df_resultado.fillna(0, inplace=True)
    df_resultado["MES"] = pd.to_datetime(df_resultado["FECHA"]).dt.month

    df_resultado = df_resultado.drop(["FECHA"], axis=1)
    df_resultado = df_resultado.drop(["COD_CLIENTE"], axis=1)

    return df_resultado


## GET PRECIOS CAMARÓN FOR PREDICTION
def get_precios_camaron_for_prediction(cursor, fecha_desde, fecha_hasta):
    """
    Retrieves precio camaron data based on given date range.

    This function executes a SQL query to fetch precio camaron data from the database. It is used to
    generate the data required for the production prediction model.

    Parameters:
        - cursor: The database cursor object.
        - fecha_desde: The start date of the date range.
        - fecha_hasta: The end date of the date range.

    Returns:
        - On success, returns a dataframe containing the precio camaron data and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    cursor.execute(
        f"""
        (SELECT 
            fecha as FECHA,
            `30-40 (29 g)`,
            `40-50 (23 g)`,
            `50-60 (18 g)`,
            `60-70 (15 g)`,
            `70-80 (13 g)`,
            `80-100 (11 g)`
        FROM precio_camaron
        WHERE fecha <= '{fecha_desde}' 
        ORDER BY fecha DESC
        LIMIT 1)

        UNION ALL

        (SELECT
            fecha as FECHA,
            `30-40 (29 g)`,
            `40-50 (23 g)`,
            `50-60 (18 g)`,
            `60-70 (15 g)`,
            `70-80 (13 g)`,
            `80-100 (11 g)`
        FROM precio_camaron
        WHERE fecha BETWEEN '{fecha_desde}' AND '{fecha_hasta}')
    """
    )
    data_precios_camaron = cursor.fetchall()
    columns_precios_camaron = [i[0] for i in cursor.description]
    dataframe_precios_camaron = pd.DataFrame(
        data_precios_camaron, columns=columns_precios_camaron
    )
    dataframe_precios_camaron["FECHA"] = pd.to_datetime(
        dataframe_precios_camaron["FECHA"]
    )

    dataframe_precios_camaron["30-40 (29 g)"] = (
        dataframe_precios_camaron["30-40 (29 g)"].astype(float)
    ) * 2000
    dataframe_precios_camaron["40-50 (23 g)"] = (
        dataframe_precios_camaron["40-50 (23 g)"].astype(float)
    ) * 2000
    dataframe_precios_camaron["50-60 (18 g)"] = (
        dataframe_precios_camaron["50-60 (18 g)"].astype(float)
    ) * 2000
    dataframe_precios_camaron["60-70 (15 g)"] = (
        dataframe_precios_camaron["60-70 (15 g)"].astype(float)
    ) * 2000
    dataframe_precios_camaron["70-80 (13 g)"] = (
        dataframe_precios_camaron["70-80 (13 g)"].astype(float)
    ) * 2000
    dataframe_precios_camaron["80-100 (11 g)"] = (
        dataframe_precios_camaron["80-100 (11 g)"].astype(float)
    ) * 2000

    date_range = pd.date_range(start=fecha_desde, end=fecha_hasta)
    df_date_range = pd.DataFrame(date_range, columns=["FECHA"])
    df_date_range["FECHA"] = pd.to_datetime(df_date_range["FECHA"])

    dataframe_precios_camaron = pd.merge_asof(
        df_date_range, dataframe_precios_camaron, on="FECHA", direction="backward"
    )
    dataframe_precios_camaron = dataframe_precios_camaron.ffill()

    return dataframe_precios_camaron


## GET EXPORTACIONES FOR PREDICTION
def get_exportaciones_for_prediction(cursor, fecha_desde, fecha_hasta):
    """
    Retrieves exportaciones data based on given date range.

    This function executes a SQL query to fetch exportaciones data from the database. It is used to
    generate the data required for the production prediction model.

    Parameters:
        - cursor: The database cursor object.
        - fecha_desde: The start date of the date range.
        - fecha_hasta: The end date of the date range.

    Returns:
        - On success, returns a dataframe containing the exportaciones data and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    cursor.execute(
        f"""
        SELECT 
            fecha,
            total_lb,
            total_fob
        FROM exportacion
        WHERE fecha BETWEEN '{fecha_desde}' AND '{fecha_hasta}'
    """
    )
    data_exportaciones = cursor.fetchall()
    columns_exportaciones = [i[0] for i in cursor.description]
    dataframe_exportaciones = pd.DataFrame(
        data_exportaciones, columns=columns_exportaciones
    )
    dataframe_exportaciones.columns = [
        column.upper() for column in dataframe_exportaciones.columns.values
    ]

    dataframe_exportaciones["TOTAL_LB"] = (
        dataframe_exportaciones["TOTAL_LB"].astype(float)
    ) / 2000
    dataframe_exportaciones["TOTAL_FOB"] = (
        dataframe_exportaciones["TOTAL_FOB"].astype(float)
    ) * 2000

    return dataframe_exportaciones


## GET VENTAS FOR PREDICTION
def get_ventas_for_prediction(cursor, fecha_desde, fecha_hasta):
    """
    Retrieves ventas data based on given date range.

    This function executes a SQL query to fetch ventas data from the database. It is used to
    generate the data required for the production prediction model.

    Parameters:
        - cursor: The database cursor object.
        - fecha_desde: The start date of the date range.
        - fecha_hasta: The end date of the date range.

    Returns:
        - On success, returns a dataframe containing the ventas data and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    ## CONSTRUCCIÓN DE DATAFRAME VACÍO
    cursor.execute(
        f"""
        SELECT
        DISTINCT familia
        FROM producto
    """
    )
    data_familias = cursor.fetchall()
    data_familias = [familia[0] for familia in data_familias]

    lineas = ["SEEDING", "VOLUMA"]
    clientes = range(1, 8)
    familias_lineas = [
        f"{familia}_{linea}" for familia in data_familias for linea in lineas
    ]
    columnas_esperadas = [
        f"{linea}_{cliente}" for cliente in clientes for linea in familias_lineas
    ]
    columnas_esperadas.insert(0, "FECHA")
    dataframe_esperado = pd.DataFrame(columns=columnas_esperadas)

    ## CONSTRUCCIÓN DE DATAFRAME CON DATA DE VENTAS
    cursor.execute(
        f"""
        SELECT 
            cod_cliente,
            familia,
            fecha_emision as fecha,
            grupo_linea,
            toneladas
        FROM venta
        INNER JOIN cliente ON venta.id_cliente = cliente.id_cliente 
        LEFT JOIN producto ON venta.id_producto = producto.id_producto
        WHERE fecha_emision BETWEEN '{fecha_desde}' AND '{fecha_hasta}'
    """
    )
    data_ventas = cursor.fetchall()
    columns_ventas = [i[0] for i in cursor.description]
    dataframe_ventas = pd.DataFrame(data_ventas, columns=columns_ventas)
    dataframe_ventas.columns = [
        column.upper() for column in dataframe_ventas.columns.values
    ]

    dataframe_ventas["COD_CLIENTE"] = dataframe_ventas["COD_CLIENTE"].astype(int)
    dataframe_ventas["FAMILIA_LINEA"] = (
        dataframe_ventas["FAMILIA"] + "_" + dataframe_ventas["GRUPO_LINEA"]
    )
    dataframe_ventas = dataframe_ventas.drop(["FAMILIA", "GRUPO_LINEA"], axis=1)

    dataframe_ventas["CLIENTE_LINEA"] = (
        dataframe_ventas["FAMILIA_LINEA"]
        + "_"
        + (dataframe_ventas["COD_CLIENTE"] - 2100000).astype(str)
    )
    lineas = dataframe_ventas["FAMILIA_LINEA"].unique()
    clientes = range(1, 8)
    columnas = [f"{linea}_{cliente}" for cliente in clientes for linea in lineas]

    dataframe_ventas = dataframe_ventas.pivot_table(
        index=["FECHA"], columns="CLIENTE_LINEA", values="TONELADAS", aggfunc="sum"
    ).fillna(0)
    dataframe_ventas = dataframe_ventas.reindex(columns=columnas, fill_value=0)
    dataframe_ventas = dataframe_ventas.reset_index()

    ## LÓGICA PARA LLENAR DATAFRAME VACÍO CON DATA DE VENTAS
    dataframe_ventas = dataframe_ventas.reindex(
        columns=columnas_esperadas, fill_value=0
    )
    df_resultado = pd.concat([dataframe_esperado, dataframe_ventas])
    df_resultado.fillna(0, inplace=True)
    df_resultado["MES"] = pd.to_datetime(df_resultado["FECHA"]).dt.month

    return df_resultado


## CHECK PREVIOUS MONTH DATA
def check_previous_month_data(connection, date, table):
    """
    Checks if previous month data exists.

    This function executes a SQL query to check if data exists for the previous month. It is used to
    validate the data upload process.

    Parameters:
        - connection: The database connection object.
        - date: The date for which the data upload is being attempted.
        - table: The table for which the data upload is being attempted.

    Returns:
        - On success, returns True if data exists for the previous month, and False otherwise.
        - On failure, returns an error message and a status code 500.
    """
    first_day_current_month = date.replace(day=1)
    first_day_previous_month = first_day_current_month - timedelta(days=1)
    first_day_previous_month = first_day_previous_month.replace(day=1)

    start_date = first_day_previous_month.strftime("%Y-%m-%d")
    end_date = first_day_current_month.strftime("%Y-%m-%d")

    if table == "venta":
        query = f"""
            SELECT COUNT(*) FROM venta
            WHERE fecha_emision >= '{start_date}' AND fecha_emision < '{end_date}'
        """
    elif table == "sow":
        query = f"""
            SELECT COUNT(*) FROM sow
            WHERE fecha_periodo >= '{start_date}' AND fecha_periodo < '{end_date}'
        """
    elif (
        table == "exportacion" or table == "precio_camaron" or table == "materia_prima"
    ):
        query = f"""
            SELECT COUNT(*) FROM exportacion
            WHERE fecha >= '{start_date}' AND fecha < '{end_date}'
        """

    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchone()
    return data[0] > 0


## CHECK ALREADY UPLOADED DATA
def check_already_uploaded_data(connection, date, table):
    """
    Checks if data has already been uploaded.

    This function executes a SQL query to check if data has already been uploaded for the given month.
    It is used to validate the data upload process.

    Parameters:
        - connection: The database connection object.
        - date: The date for which the data upload is being attempted.
        - table: The table for which the data upload is being attempted.

    Returns:
        - On success, returns True if data exists for the given month, and False otherwise.
        - On failure, returns an error message and a status code 500.
    """
    first_day_month = date.replace(day=1)

    first_day_next_month = (
        first_day_month.replace(month=first_day_month.month + 1, day=1)
        if first_day_month.month < 12
        else first_day_month.replace(year=first_day_month.year + 1, month=1, day=1)
    )
    last_day_month = first_day_next_month - timedelta(days=1)

    start_date = first_day_month.strftime("%Y-%m-%d")
    end_date = last_day_month.strftime("%Y-%m-%d")

    if table == "venta":
        print(start_date, end_date)
        query = f"""
            SELECT COUNT(*) FROM venta

            WHERE fecha_emision >= '{start_date}' AND fecha_emision <= '{end_date}'
        """
    elif table == "sow":
        query = f"""
            SELECT COUNT(*) FROM sow
            WHERE fecha_periodo >= '{start_date}' AND fecha_periodo <= '{end_date}'
        """
    elif (
        table == "exportacion" or table == "precio_camaron" or table == "materia_prima"
    ):
        query = f"""
            SELECT COUNT(*) FROM {table}
            WHERE fecha >= '{start_date}' AND fecha <= '{end_date}'
        """

    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchone()
    return data[0] > 0


## UPLOAD VENTAS DATA
def upload_ventas_data(connection, data):
    """
    Uploads ventas data to the database.

    This function executes a series of SQL queries to upload ventas data to the database. It is used to
    upload ventas data from an Excel file to the database.

    Parameters:
        - connection: The database connection object.
        - data: The ventas data to be uploaded.

    Returns:
        - On success, returns a success message and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    try:
        data_ventas = pd.DataFrame(pd.read_excel(data))
        cursor = connection.cursor()

        if any(
            check_already_uploaded_data(connection, row["FEC_EMISION"], "venta")
            for _, row in data_ventas.iterrows()
        ):
            return "Ya existe data para el mes que se desea cargar", 400

        if not all(
            check_previous_month_data(connection, row["FEC_EMISION"], "venta")
            for _, row in data_ventas.iterrows()
        ):
            return "No existe data para el mes anterior", 400

        for index, row in data_ventas.iterrows():
            fecha = row["FEC_EMISION"].strftime("%Y-%m-%d")
            toneladas = row["TON"]

            insert_venta = f"""
                INSERT INTO venta (id_cliente, id_producto, fecha_emision, toneladas)
                VALUES (
                    (SELECT id_cliente FROM cliente WHERE cod_cliente = '{row['COD_CLIENTE']}'),
                    (SELECT id_producto FROM producto WHERE cod_sku = '{row['COD_SKU']}'),
                    '{fecha}',
                    '{toneladas}'
                )
            """
            cursor.execute(insert_venta)

        cursor.execute("COMMIT")
        cursor.close()

        return "Data uploaded successfully", 200
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(traceback.format_exc())
        return "Something went wrong", 500


## UPLOAD MATERIA PRIMA DATA
def upload_materia_prima_data(connection, data):
    """
    Uploads materia prima data to the database.

    This function executes a series of SQL queries to upload materia prima data to the database. It is used to
    upload materia prima data from an Excel file to the database.

    Parameters:
        - connection: The database connection object.
        - data: The materia prima data to be uploaded.

    Returns:
        - On success, returns a success message and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    try:
        data_materia_prima = pd.DataFrame(pd.read_excel(data))
        cursor = connection.cursor()

        if any(
            check_already_uploaded_data(connection, row["FEC_EMISION"], "materia_prima")
            for _, row in data_materia_prima.iterrows()
        ):
            return "Ya existe data para el mes que se desea cargar", 400

        if not all(
            check_previous_month_data(connection, row["FEC_EMISION"], "materia_prima")
            for _, row in data_materia_prima.iterrows()
        ):
            return "No existe data para el mes anterior", 400

        for index, row in data_materia_prima.iterrows():
            cursor.execute(
                f"""
                INSERT INTO materia_prima (fecha, total_usd_lecitina, libras_neto_lecitina, total_usd_soya, libras_neto_soya, total_usd_trigo, libras_neto_trigo)
                VALUES (
                    '{row['Fecha']}',
                    {row['Total USD Lecitina']},
                    {row['Libras Neto Lecitina']},
                    {row['Total USD Soya']},
                    {row['Libras Neto Soya']},
                    {row['Total USD Trigo']},
                    {row['Libras Neto Trigo']}
                )
            """
            )

        cursor.execute("COMMIT")
        cursor.close()
        return "Data uploaded successfully", 200
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(traceback.format_exc())
        return "Something went wrong", 500


## UPLOAD PRECIOS CAMARÓN DATA
def upload_precio_camaron_data(connection, data):
    """
    Uploads precio camaron data to the database.

    This function executes a series of SQL queries to upload precio camaron data to the database. It is used to
    upload precio camaron data from an Excel file to the database.

    Parameters:
        - connection: The database connection object.
        - data: The precio camaron data to be uploaded.

    Returns:
        - On success, returns a success message and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    try:
        data_precios_camaron = pd.DataFrame(pd.read_excel(data))
        cursor = connection.cursor()

        if any(
            check_already_uploaded_data(
                connection, row["FEC_EMISION"], "precio_camaron"
            )
            for _, row in data_precios_camaron.iterrows()
        ):
            return "Ya existe data para el mes que se desea cargar", 400

        if not all(
            check_previous_month_data(connection, row["FEC_EMISION"], "precio_camaron")
            for _, row in data_precios_camaron.iterrows()
        ):
            return "No existe data para el mes anterior", 400

        for row in data:
            cursor.execute(
                f"""
                INSERT INTO precio_camaron (fecha, `30-40 (29 g)`, `40-50 (23 g)`, `50-60 (18 g)`, `60-70 (15 g)`, `70-80 (13 g)`, `80-100 (11 g)`)
                VALUES (
                    '{row['Fecha']}',
                    {row['30-40 (29 g)']},
                    {row['40-50 (23 g)']},
                    {row['50-60 (18 g)']},
                    {row['60-70 (15 g)']},
                    {row['70-80 (13 g)']},
                    {row['80-100 (11 g)']}
                )
            """
            )

        cursor.execute("COMMIT")
        cursor.close()
        return "Data uploaded successfully", 200
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(traceback.format_exc())
        return "Something went wrong", 500


## UPLOAD EXPORTACIONES DATA
def upload_exportaciones_data(connection, data):
    """
    Uploads exportaciones data to the database.

    This function executes a series of SQL queries to upload exportaciones data to the database. It is used to
    upload exportaciones data from an Excel file to the database.

    Parameters:
        - connection: The database connection object.
        - data: The exportaciones data to be uploaded.

    Returns:
        - On success, returns a success message and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    try:
        data_exportaciones = pd.DataFrame(pd.read_excel(data))
        cursor = connection.cursor()

        if any(
            check_already_uploaded_data(connection, row["FEC_EMISION"], "exportacion")
            for _, row in data_exportaciones.iterrows()
        ):
            return "Ya existe data para el mes que se desea cargar", 400

        if not all(
            check_previous_month_data(connection, row["FEC_EMISION"], "exportacion")
            for _, row in data_exportaciones.iterrows()
        ):
            return "No existe data para el mes anterior", 400

        for row in data:
            cursor.execute(
                f"""
                INSERT INTO exportacion (fecha, total_lb, total_fob)
                VALUES (
                    '{row['Fecha']}',
                    {row['Total LB']},
                    {row['Total FOB']}
                )
            """
            )

        cursor.execute("COMMIT")
        cursor.close()
        return "Data uploaded successfully", 200
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(traceback.format_exc())
        return "Something went wrong", 500


## UPLOAD SOW DATA
def upload_sow_data(connection, data):
    """
    Uploads sow data to the database.

    This function executes a series of SQL queries to upload sow data to the database. It is used to
    upload sow data from an Excel file to the database.

    Parameters:
        - connection: The database connection object.
        - data: The sow data to be uploaded.

    Returns:
        - On success, returns a success message and a status code 200.
        - On failure, returns an error message and a status code 500.
    """
    try:
        cursor = connection.cursor()
        data_sow = pd.DataFrame(pd.read_excel(data))

        if any(
            check_already_uploaded_data(connection, row["FEC_EMISION"], "sow")
            for _, row in data_sow.iterrows()
        ):
            return "Ya existe data para el mes que se desea cargar", 400

        if not all(
            check_previous_month_data(connection, row["FEC_EMISION"], "sow")
            for _, row in data_sow.iterrows()
        ):
            return "No existe data para el mes anterior", 400

        for index, row in data_sow.iterrows():
            fecha = row["FEC_EMISION"].strftime("%Y-%m-%d")
            cursor.execute(
                f"""
                INSERT INTO sow (id_cliente, fecha_periodo, potencial_grupo, nicovita, sow_max_alcanzable)
                VALUES (
                    (SELECT id_cliente FROM cliente WHERE cod_cliente = '{row['COD_CLIENTE']}'),
                    '{fecha}',
                    {row['POTENCIAL_GRUPO']},
                    {row['NICOVITA']},
                    {row['SOW_MAX_ALCANZABLE']}
                )
            """
            )

        cursor.execute("COMMIT")
        cursor.close()
        return "Data uploaded successfully", 200
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(traceback.format_exc())
        return "Something went wrong", 500
