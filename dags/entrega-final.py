import requests
import redshift_connector as rc
import pandas as pd 

def obtener_informacion_inflacion(url, cursor, conexion_redshift):
    # Solicitar información a la API Pública
    response = requests.get(url)

    # Verificar si la solicitud fue correcta
    if response.status_code == 200:
        # Convertir la respuesta a formato JSON
        data = response.json()

        # Crear un DataFrame de Pandas para manipular los datos 
        df = pd.DataFrame(data['data'])
        df['name'] = data['name']
        df['interval'] = data['interval']
        df['unit'] = data['unit']

        # Eliminar duplicados si existen  
        df.drop_duplicates(inplace=True)

        # Imprimir la información
        #print(df)

        # Insertar datos en tabla
        query_insercion = '''
            INSERT INTO indicator_data (name, interval, unit, date, value)
            VALUES '''

        for index, row in df.iterrows():
            # Generar una nueva entrada para cada fila del DataFrame
            indicator_data_string = "('{0}', '{1}', '{2}', '{3}', {4}),".format(
                row['name'], row['interval'], row['unit'], row['date'], row['value']
            )
            query_insercion += indicator_data_string
        
        # Elimino la ',' de mas al final, y agrego ';' para indicar el fin de la sentencia
        query_insercion = query_insercion[:-1]
        query_insercion += ";"

        # Ejecuto la sentencia
        cursor.execute(query_insercion)
        conexion_redshift.commit()
        
    else:
        print(f"Error al obtener datos de la API. Código de estado: {response.status_code}")

def main():
    print("-------- Entrega Final --------")
    # Conectarse a Redshift 
    try: # Evitamos que el programa crashee en caso de una conexion fallida

        # Configuración Redshift
        redshift_params = {
            'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            'database': 'data-engineer-database',
            'port': '5439',
            'user': 'josefina24carlos_coderhouse', 
            'password': 'Wlh1OQ34ir'
        } # Verificar conexion. 

        # Crear un cursor
        conexion_redshift = rc.connect(**redshift_params)
        cursor = conexion_redshift.cursor()

        # Crear tabla
        crear_tabla_query = '''
        CREATE TABLE IF NOT EXISTS indicator_data (
            id INT PRIMARY KEY IDENTITY(1,1),
            name VARCHAR(255),
            interval VARCHAR(50),
            unit VARCHAR(50),
            date DATE,
            value FLOAT
        );
        '''
        cursor.execute(crear_tabla_query)
        conexion_redshift.commit()

        # Llamar a la función para obtener datos de la API
        url = 'https://www.alphavantage.co/query?function=INFLATION&apikey=F2FT4DJMFABFKRQC' 
        obtener_informacion_inflacion(url, cursor, conexion_redshift)

        # Cerrar la conexión
        cursor.close()
        conexion_redshift.close()
    except Exception as e:
        print(f"Error al conectar a Redshift: {e}") # imprimir error si conexion falla

main()