import requests
import redshift_connector as rc

def obtener_informacion_inflacion(url, cursor, conexion_redshift):
    # Solicitar información a la API Pública
    response = requests.get(url)

    # Verificar si la solicitud fue correcta
    if response.status_code == 200:
        # Convertir la respuesta a formato JSON
        data = response.json()

        # Imprimir la información
        print(f"Nombre: {data['name']}")
        print(f"Intervalo de tiempo: {data['interval']}")
        print(f"Porcentaje: {data['unit']}")

        # Guardo la data obtenida en un arreglo
        data_array = data['data']

        query_insercion = '''
            INSERT INTO indicator_data (name, interval, unit, date, value)
            VALUES '''

        for data_val in data_array:
            # Genero una nueva entrada para cada valor obtenido
            indicator_data_string = "('{0}', '{1}', '{2}', '{3}', {4}),".format(
                data['name'], data['interval'], data['unit'], data_val['date'], data_val['value']
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
    print("SegundaEntrega")
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
