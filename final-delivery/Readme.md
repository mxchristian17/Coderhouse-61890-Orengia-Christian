# Proyecto entregable CoderHouse - Data engineer
## Detalles
- **Alumno:** Orengia Christian Ezequiel
- **Curso:** Data Engineer
- **Comisión:** 61890
- **Mail:** orengiachristian@gmail.com

## Descripción del proyecto
ETL de las APIS públicas
- [Population.io](https://publicapis.io/population-io-api/)
- [Open-Meteo](https://open-meteo.com/en/docs)

### Obtención de API Keys necesarias
- 
## Requerimientos
- Para la ejecución de este proyecto, se debe disponer de Docker instalado en el equipo.
## Documentacion de uso de la aplicación
1. Completar el archivo .env considerando como ejemplo el archivo .env.example
2. Ejecutar el Makefile a traves de la shell con el comando `make build` luego de posicionar el puntero en el directorio raiz del proyecto.




Para el proyecto final usar version 2.9 al menos
Mandar mails si falla
Mandar mails si termina la ejecucion
Validar que haya ingestado correctamente
El DAG tiene que estar completamente armado
Utilizar Redshift
El proyecto debe estar construido en contendores
Debe haber una buena documentación para las tablas de las bd el Readme.md esta bien
Mail sender o sendgrid o la cuenta de google para mandar los mails esta bien.
Es obligatorio que envie correos

Por un lado dejar un archivo sql con los archivos que crean la tabla init.sql y redshift_table.sql por ejemplo
Ver ejemplo Semana 12 proyectofinal
Tiene que haber una carpeta dags. Dentro los dags por ejemplo dag_etl.py y una carpeta modules

Modules: data_extract.py, data_load.py, data_transform.py, mail_sender.py, ...
Crear un Taskfile.yml (Ver ejemplo del prouyecto de muestra del profesor)
En lo personal me gusta mas el makefile que el taskfile. Mucho mas facil a mi criterio y la funcionalidad no parece ser muy distinta.
