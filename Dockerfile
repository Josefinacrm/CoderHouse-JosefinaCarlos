FROM tomcat:9.0

# Copiar archivo última-entrega.py al directorio webapps de Tomcat

COPY dags/entrega-final.py /usr/local/tomcat/webapps/ 

# Exponer el puerto 8080
EXPOSE 8080

# CMD inicia el servidor Tomcat
CMD ["catalina.sh", "run"]
