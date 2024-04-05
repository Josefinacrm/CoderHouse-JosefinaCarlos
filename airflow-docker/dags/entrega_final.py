import requests
import redshift_connector as rc
import pandas as pd 
from airflow.models import Variable, DAG 
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def obtener_informacion_inflacion(url, cursor, conexion_redshift):
    # Solicitar información a la API 
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
            'user': Variable.get("user_redshift"),
            'password': Variable.get("secret_pass_redshift")
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
        url = 'https://www.alphavantage.co/query?function=INFLATION&apikey=' + Variable.get("alpha_vantage_api_key")
        obtener_informacion_inflacion(url, cursor, conexion_redshift)

        # Cerrar la conexión
        cursor.close()
        conexion_redshift.close()
    except Exception as e:
        print(f"Error al conectar a Redshift: {e}") # imprimir error si conexion falla

def send_email():
    try:
        Pass_Email = Variable.get("secret_pass_gmail")
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        sender_email = 'josefinacarlosfotografia@gmail.com'
        password = Pass_Email

        subject = 'Carga de datos'
        body_text = 'Los datos fueron cargados a la base de datos correctamente.'

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = sender_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body_text, 'plain'))
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
        
        print('El email fue enviado correctamente.')

    except Exception as exception:
        print(exception)
        print('El email no se pudo enviar.')

main()