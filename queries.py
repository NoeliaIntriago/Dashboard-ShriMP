from datetime import datetime, timedelta

import pandas as pd
import traceback

## GET MIN_DATE AND MAX_DATE
def get_min_max_date(connection):
    try:
        cursor = connection.cursor()
        cursor.execute('SELECT MIN(fecha_emision) as MIN_DATE, MAX(fecha_emision) as MAX_DATE FROM venta')
        data = cursor.fetchone()
        cursor.close()

        return data, 200
    except Exception as e:
        print(traceback.format_exc())
        return 'Something went wrong', 500

## GET ALL CLIENTS
def get_clients(connection):
    try: 
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM cliente')
        data = [row[2] for row in cursor.fetchall()]
        cursor.close()

        return data, 200
    except Exception as e:
        print(traceback.format_exc())
        return 'Something went wrong', 500
    
## GET HISTORY  
def get_historic(connection, params):
    try:
        cursor = connection.cursor()
        base_query = '''
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
        '''
        start_date = datetime(int(params['year']), int(params['month']), 1).strftime('%Y-%m-%d')

        if (int(params['month']) == 12):
            end_date = datetime(int(params['year']) + 1, 1, 1).strftime('%Y-%m-%d')
        else:
            end_date = datetime(int(params['year']), int(params['month']) + 1, 1).strftime('%Y-%m-%d')

        conditions = []

        conditions.append(f"venta.fecha_emision BETWEEN '{start_date}' AND '{end_date}'")

        if params['stage'] is not None:
            conditions.append(f"producto.grupo_linea = '{params['stage']}'")

        if params['client'] is not None:
            conditions.append(f"cliente.des_cliente = '{params['client']}'")

        final_query = f"{base_query} WHERE {' AND '.join(conditions)}"

        cursor.execute(final_query)
        
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        cursor.close()

        formatted_data = [
            {columns[i]: row[i] for i in range(len(columns))} 
            for row in data
        ]

        return formatted_data, 200
    except Exception as e:
        print(traceback.format_exc())
        return 'Something went wrong', 500

## GET PREDICTION
def get_prediction_data(connection, date):
    fecha_desde = date - timedelta(weeks=4)
    fecha_hasta = date - timedelta(days=1)

    try:
        dataframe_mp = get_materia_prima_for_prediction(connection.cursor(), fecha_desde, fecha_hasta)
        dataframe_sow = get_sow_for_prediction(connection.cursor(), fecha_desde, fecha_hasta)
        dataframe_precios_camaron = get_precios_camaron_for_prediction(connection.cursor(), fecha_desde, fecha_hasta)
        dataframe_exportaciones = get_exportaciones_for_prediction(connection.cursor(), fecha_desde, fecha_hasta)
        dataframe_ventas = get_ventas_for_prediction(connection.cursor(), fecha_desde, fecha_hasta)

        merge_ventas_sow = pd.merge(dataframe_ventas, dataframe_sow, on='MES', how='left')
        merge_ventas_sow = merge_ventas_sow.drop(['MES'], axis=1)

        merge_ventas_sow['FECHA'] = pd.to_datetime(merge_ventas_sow['FECHA']).dt.strftime('%Y-%m-%d')
        dataframe_exportaciones['FECHA'] = pd.to_datetime(dataframe_exportaciones['FECHA']).dt.strftime('%Y-%m-%d')
        dataframe_mp['FECHA'] = pd.to_datetime(dataframe_mp['FECHA']).dt.strftime('%Y-%m-%d')
        dataframe_precios_camaron['FECHA'] = pd.to_datetime(dataframe_precios_camaron['FECHA']).dt.strftime('%Y-%m-%d')

        merged_data = pd.merge(merge_ventas_sow, dataframe_exportaciones, on='FECHA', how='left')
        merged_data = pd.merge(merged_data, dataframe_mp, on='FECHA', how='left')
        merged_data = pd.merge(merged_data, dataframe_precios_camaron, on='FECHA', how='left')

        merged_data = merged_data.set_index('FECHA')

        return merged_data, 200
        
    except Exception as e:
        print(traceback.format_exc())
        return 'Something went wrong', 500
    
## GET MATERIA PRIMA INFORMATION FOR PREDICTION
def get_materia_prima_for_prediction(cursor, fecha_desde, fecha_hasta):
    cursor.execute(f'''
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
    ''')
    data_mp = cursor.fetchall()
    columns_mp = [i[0] for i in cursor.description]
    dataframe_mp = pd.DataFrame(data_mp, columns=columns_mp)
    dataframe_mp.columns = ['FECHA', 'TOTAL_USD_LECITINA', 'LIBRAS_NETO_LECITINA', 'TOTAL_USD_SOYA', 'LIBRAS_NETO_SOYA', 'TOTAL_USD_TRIGO', 'LIBRAS_NETO_TRIGO']

    dataframe_mp['LIBRAS_NETO_LECITINA'] = (dataframe_mp['LIBRAS_NETO_LECITINA'].astype(float)) / 2000
    dataframe_mp['LIBRAS_NETO_SOYA'] = (dataframe_mp['LIBRAS_NETO_SOYA'].astype(float)) / 2000
    dataframe_mp['LIBRAS_NETO_TRIGO'] = (dataframe_mp['LIBRAS_NETO_TRIGO'].astype(float)) / 2000

    dataframe_mp['TOTAL_USD_LECITINA'] = (dataframe_mp['TOTAL_USD_LECITINA'].astype(float)) * 2000
    dataframe_mp['TOTAL_USD_SOYA'] = (dataframe_mp['TOTAL_USD_SOYA'].astype(float)) * 2000
    dataframe_mp['TOTAL_USD_TRIGO'] = (dataframe_mp['TOTAL_USD_TRIGO'].astype(float)) * 2000

    return dataframe_mp

## GET SOW INFORMATION FOR PREDICTION
def get_sow_for_prediction(cursor, fecha_desde, fecha_hasta):
    columnas = ['NICOVITA', 'POTENCIAL_GRUPO', 'SOW_MAX_ALCANZABLE']
    clientes = range(1, 8)

    columnas_esperadas = [f'{column}_{cliente}' for cliente in clientes for column in columnas]
    columnas_esperadas.insert(0, 'FECHA')
    columnas_esperadas.insert(1, 'COD_CLIENTE')
    dataframe_esperado = pd.DataFrame(columns=columnas_esperadas)

    # CONSTRUCCIÓN DE DATAFRAME CON DATA DE SOW
    cursor.execute(f'''
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
    ''')
    data_sow = cursor.fetchall()
    columns_sow = [i[0] for i in cursor.description]
    dataframe_sow = pd.DataFrame(data_sow, columns=columns_sow)
    dataframe_sow.columns = [column.upper() for column in dataframe_sow.columns.values]
    dataframe_sow['MES'] = pd.to_datetime(dataframe_sow['FECHA']).dt.month
    
    dataframe_sow = dataframe_sow.groupby(['FECHA', 'COD_CLIENTE']).agg({'POTENCIAL_GRUPO': 'sum', 'NICOVITA': 'mean', 'SOW_MAX_ALCANZABLE': 'mean'}).reset_index()
    dataframe_sow = dataframe_sow.pivot_table(index=['FECHA'], columns='COD_CLIENTE', values=['POTENCIAL_GRUPO', 'NICOVITA', 'SOW_MAX_ALCANZABLE'], aggfunc='first').fillna(0)
    new_columns = {}
    for column in dataframe_sow.columns:
        if 'NICOVITA' in column:
            new_columns[column] = f'NICOVITA_{column[-1]}'
        elif 'POTENCIAL_GRUPO' in column:
            new_columns[column] = f'POTENCIAL_GRUPO_{column[-1]}'
        elif 'SOW_MAX_ALCANZABLE' in column:
            new_columns[column] = f'SOW_MAX_ALCANZABLE_{column[-1]}'

    dataframe_sow = dataframe_sow.rename(columns=new_columns)
    dataframe_sow.columns = [f'{column[0]}_{column[1][-1]}' for column in dataframe_sow.columns.values]
    dataframe_sow = dataframe_sow.reset_index()

    # LÓGICA PARA LLENAR DATAFRAME VACÍO CON DATA DE SOW
    dataframe_sow = dataframe_sow.reindex(columns=columnas_esperadas, fill_value=0)
    df_resultado = pd.concat([dataframe_esperado, dataframe_sow])
    df_resultado.fillna(0, inplace=True)
    df_resultado['MES'] = pd.to_datetime(df_resultado['FECHA']).dt.month

    df_resultado = df_resultado.drop(['FECHA'], axis=1)
    df_resultado = df_resultado.drop(['COD_CLIENTE'], axis=1)

    return df_resultado

## GET PRECIOS CAMARÓN FOR PREDICTION
def get_precios_camaron_for_prediction(cursor, fecha_desde, fecha_hasta):
    cursor.execute(f'''
        SELECT 
            fecha as FECHA,
            `30-40 (29 g)`,
            `40-50 (23 g)`,
            `50-60 (18 g)`,
            `60-70 (15 g)`,
            `70-80 (13 g)`,
            `80-100 (11 g)`
        FROM precio_camaron
        WHERE fecha >= '{fecha_desde}' 
        AND fecha <= (
            SELECT MIN(fecha)
            FROM precio_camaron
            WHERE fecha >= '{fecha_hasta}'
        )
    ''')
    data_precios_camaron = cursor.fetchall()
    columns_precios_camaron = [i[0] for i in cursor.description]
    dataframe_precios_camaron = pd.DataFrame(data_precios_camaron, columns=columns_precios_camaron)
    dataframe_precios_camaron['FECHA'] = pd.to_datetime(dataframe_precios_camaron['FECHA'])

    dataframe_precios_camaron['30-40 (29 g)'] = (dataframe_precios_camaron['30-40 (29 g)'].astype(float)) * 2000
    dataframe_precios_camaron['40-50 (23 g)'] = (dataframe_precios_camaron['40-50 (23 g)'].astype(float)) * 2000
    dataframe_precios_camaron['50-60 (18 g)'] = (dataframe_precios_camaron['50-60 (18 g)'].astype(float)) * 2000
    dataframe_precios_camaron['60-70 (15 g)'] = (dataframe_precios_camaron['60-70 (15 g)'].astype(float)) * 2000
    dataframe_precios_camaron['70-80 (13 g)'] = (dataframe_precios_camaron['70-80 (13 g)'].astype(float)) * 2000
    dataframe_precios_camaron['80-100 (11 g)'] = (dataframe_precios_camaron['80-100 (11 g)'].astype(float)) * 2000

    date_range = pd.date_range(start=fecha_desde, end=fecha_hasta)
    df_date_range = pd.DataFrame(date_range, columns=['FECHA'])
    df_date_range['FECHA'] = pd.to_datetime(df_date_range['FECHA'])

    dataframe_precios_camaron = pd.merge_asof(df_date_range, dataframe_precios_camaron, on='FECHA', direction='forward')
    dataframe_precios_camaron = dataframe_precios_camaron.ffill()

    return dataframe_precios_camaron

## GET EXPORTACIONES FOR PREDICTION
def get_exportaciones_for_prediction(cursor, fecha_desde, fecha_hasta):
    cursor.execute(f'''
        SELECT 
            fecha,
            total_lb,
            total_fob
        FROM exportacion
        WHERE fecha BETWEEN '{fecha_desde}' AND '{fecha_hasta}'
    ''')
    data_exportaciones = cursor.fetchall()
    columns_exportaciones = [i[0] for i in cursor.description]
    dataframe_exportaciones = pd.DataFrame(data_exportaciones, columns=columns_exportaciones)
    dataframe_exportaciones.columns = [column.upper() for column in dataframe_exportaciones.columns.values]

    dataframe_exportaciones['TOTAL_LB'] = (dataframe_exportaciones['TOTAL_LB'].astype(float)) / 2000
    dataframe_exportaciones['TOTAL_FOB'] = (dataframe_exportaciones['TOTAL_FOB'].astype(float)) * 2000

    return dataframe_exportaciones

## GET VENTAS FOR PREDICTION
def get_ventas_for_prediction(cursor, fecha_desde, fecha_hasta):
    ## CONSTRUCCIÓN DE DATAFRAME VACÍO
    cursor.execute(f'''
        SELECT
        DISTINCT familia
        FROM producto
    ''')
    data_familias = cursor.fetchall()
    data_familias = [familia[0] for familia in data_familias]

    lineas = ['SEEDING', 'VOLUMA']
    clientes = range(1, 8)
    familias_lineas = [f'{familia}_{linea}' for familia in data_familias for linea in lineas]
    columnas_esperadas = [f'{linea}_{cliente}' for cliente in clientes for linea in familias_lineas]
    columnas_esperadas.insert(0, 'FECHA')
    dataframe_esperado = pd.DataFrame(columns=columnas_esperadas)
    
    ## CONSTRUCCIÓN DE DATAFRAME CON DATA DE VENTAS
    cursor.execute(f'''
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
    ''')
    data_ventas = cursor.fetchall()
    columns_ventas = [i[0] for i in cursor.description]
    dataframe_ventas = pd.DataFrame(data_ventas, columns=columns_ventas)
    dataframe_ventas.columns = [column.upper() for column in dataframe_ventas.columns.values]

    dataframe_ventas['COD_CLIENTE'] = dataframe_ventas['COD_CLIENTE'].astype(int)
    dataframe_ventas['FAMILIA_LINEA'] = dataframe_ventas['FAMILIA'] + '_' + dataframe_ventas['GRUPO_LINEA']
    dataframe_ventas = dataframe_ventas.drop(['FAMILIA', 'GRUPO_LINEA'], axis=1)

    dataframe_ventas['CLIENTE_LINEA'] = dataframe_ventas['FAMILIA_LINEA'] + '_' + (dataframe_ventas['COD_CLIENTE'] - 2100000).astype(str)
    lineas = dataframe_ventas['FAMILIA_LINEA'].unique()
    clientes = range(1, 8)
    columnas = [f'{linea}_{cliente}' for cliente in clientes for linea in lineas]

    dataframe_ventas = dataframe_ventas.pivot_table(index=['FECHA'], columns='CLIENTE_LINEA', values='TONELADAS', aggfunc='sum').fillna(0)
    dataframe_ventas = dataframe_ventas.reindex(columns=columnas, fill_value=0)
    dataframe_ventas = dataframe_ventas.reset_index()

    ## LÓGICA PARA LLENAR DATAFRAME VACÍO CON DATA DE VENTAS
    dataframe_ventas = dataframe_ventas.reindex(columns=columnas_esperadas, fill_value=0)
    df_resultado = pd.concat([dataframe_esperado, dataframe_ventas])
    df_resultado.fillna(0, inplace=True)
    df_resultado['MES'] = pd.to_datetime(df_resultado['FECHA']).dt.month

    return df_resultado

## CHECK IF CLIENT EXISTS
def check_client_exists(cursor, cod_znje, des_znje):
    cursor.execute(f"SELECT id_cliente FROM cliente WHERE cod_cliente = '{cod_znje}'")
    id_cliente = cursor.fetchone()

    if id_cliente is None:
        cursor.execute("INSERT INTO cliente (cod_cliente, des_cliente) VALUES (%s, %s)", (cod_znje, des_znje))
        cursor.execute("SELECT id_cliente FROM cliente WHERE cod_cliente = %s", (cod_znje,))
        id_cliente = cursor.fetchone()
    
    return id_cliente[0]

## CHECK IF PRODUCT EXISTS
def check_product_exists(cursor, cod_sku, des_sku, porcentaje_proteina, val_formato, familia, grupo_linea, familia_linea):
    cursor.execute(f"SELECT id_producto FROM producto WHERE cod_sku = '{cod_sku}'")
    id_producto = cursor.fetchone()

    if id_producto is None:
        porcentaje_proteina = float(porcentaje_proteina.replace('%', '')) / 100
        cursor.execute(f'''
            INSERT INTO producto (cod_sku, des_sku, porcentaje_proteina, val_formato, familia, grupo_linea, familia_linea) 
            VALUES ('{cod_sku}', '{des_sku}', '{porcentaje_proteina}', '{val_formato}', '{familia}', '{grupo_linea}', '{familia_linea}')
        ''')
        cursor.execute(f"SELECT id_producto FROM producto WHERE cod_sku = '{cod_sku}'")
        id_producto = cursor.fetchone()
    
    return id_producto[0]

## UPLOAD VENTAS DATA
def upload_ventas_data(connection, data):
    try:
        data_ventas = pd.DataFrame(pd.read_excel(data))
        cursor = connection.cursor()

        for index, row in data_ventas.iterrows():
            id_cliente = check_client_exists(cursor, row['COD_ZNJE'], row['DES_ZNJE'])
            id_producto = check_product_exists(cursor, row['COD_SKU'], row['DES_SKU'], row['PORC_PROTEINA'], row['VAL_FORMATO'], row['DES_FAMILIA'], row['DES_GRUPO_LINEA'], row['DES_FAMILIA'] + '_' + row['DES_LINEA'])

            fecha = datetime.strptime(row['FEC_EMISION'], '%d/%m/%Y').strftime('%Y-%m-%d')
            toneladas = row['TOT_PESO_FACTURADO']
            
            insert_venta = f'''
                INSERT INTO venta (id_cliente, id_producto, fecha_emision, toneladas)
                VALUES ('{id_cliente}', '{id_producto}', '{fecha}', '{toneladas}')
            '''
            cursor.execute(insert_venta)

        cursor.execute('COMMIT')
        cursor.close()

        return 'Data uploaded successfully', 200
    except Exception as e:
        cursor.execute('ROLLBACK')
        print(traceback.format_exc())
        return 'Something went wrong', 500
    
## UPLOAD MATERIA PRIMA DATA
def upload_materia_prima_data(connection, data):
    try:
        cursor = connection.cursor()

        for row in data:
            cursor.execute(f'''
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
            ''')
            connection.commit()

        cursor.close()
        return 'Data uploaded successfully', 200
    except Exception as e:
        print(traceback.format_exc())
        return 'Something went wrong', 500
    
## UPLOAD PRECIOS CAMARÓN DATA
def upload_precio_camaron_data(connection, data):
    try:
        cursor = connection.cursor()

        for row in data:
            cursor.execute(f'''
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
            ''')
            connection.commit()

        cursor.close()
        return 'Data uploaded successfully', 200
    except Exception as e:
        print(traceback.format_exc())
        return 'Something went wrong', 500
    
## UPLOAD EXPORTACIONES DATA
def upload_exportaciones_data(connection, data):
    try:
        cursor = connection.cursor()

        for row in data:
            cursor.execute(f'''
                INSERT INTO exportacion (fecha, total_lb, total_fob)
                VALUES (
                    '{row['Fecha']}',
                    {row['Total LB']},
                    {row['Total FOB']}
                )
            ''')
            connection.commit()

        cursor.close()
        return 'Data uploaded successfully', 200
    except Exception as e:
        print(traceback.format_exc())
        return 'Something went wrong', 500
    
## UPLOAD SOW DATA
def upload_sow_data(connection, data):
    try:
        cursor = connection.cursor()

        for row in data:
            cursor.execute(f'''
                INSERT INTO sow (id_cliente, fecha_periodo, potencial_grupo, nicovita, sow_max_alcanzable)
                VALUES (
                    (SELECT id_cliente FROM cliente WHERE des_cliente = '{row['Cliente']}'),
                    '{row['Fecha']}',
                    {row['Potencial Grupo']},
                    {row['Nicovita']},
                    {row['SOW Max Alcanzable']}
                )
            ''')
            connection.commit()

        cursor.close()
        return 'Data uploaded successfully', 200
    except Exception as e:
        print(traceback.format_exc())
        return 'Something went wrong', 500