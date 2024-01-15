from datetime import datetime
import traceback

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
    print("Predicting...")