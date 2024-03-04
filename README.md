Explicación del código:
En el presente repositorio se encuentra la segunda entrega del curso Data Engineering de Coderhouse.
La API que se seleccionó es una API a la cual se accede mediante una Key gratuita y que contiene información sobre la inflación en Estados Unidos y como va progresando temporalmente.
Mediante la ejecución del código en el idioma Python se indica la creación de una tabla (indicator_data) en Redshift, la cual contendrá los datos extraidos de la API.

Pasos-Para entrega 3:
#Abrir terminal docker build -t josefina_carlos_imagen .
docker image ls
docker run josefina_carlos_imagen
#Abrir otra terminal
mkdir airflow_docker
cd airflow_docker
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.3/docker-compose.yml'
mkdir -p dags logs plugins
docker-compose up
