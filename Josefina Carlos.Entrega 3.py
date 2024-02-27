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
        print(df)

        # Insertar los datos en Redshift
        df.to_sql('indicator_data', conexion_redshift, if_exists='append', index=False)

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
        } 

        # Crear un cursor
        conexion_redshift = rc.connect(**redshift_params)

        # Llamar a la función para obtener datos de la API
        url = 'https://www.alphavantage.co/query?function=INFLATION&apikey=F2FT4DJMFABFKRQC' 
        obtener_informacion_inflacion(url, None, conexion_redshift)

        # Cerrar la conexión
        conexion_redshift.close()
    except Exception as e:
        print(f"Error al conectar a Redshift: {e}") # imprimir error si conexion falla

if __name__ == "__main__":
    main()
