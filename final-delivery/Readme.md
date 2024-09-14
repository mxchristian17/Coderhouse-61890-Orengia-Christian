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

## Notas para el profesor
- En el archivo table_management.py del directorio dags_modules se encuentran las funciones para la creación de las tablas de Redshift.
- El programa verifica si las tablas ya existen y sino las crea. Además, en el archivo etl.py, hay una variable `default_delete_old_data = False` que si se pasa a true, ejecutará el borrado de las tablas antes de la ejecución de la ETL cada vez. No tiene tanto sentido para la ejecución por Airflow, pero tiene sentido a los fines de desarrollo y testeo.
- En el archivo transform.py dentro del directorio dag_modules se puede optar por correr una versión con pocos registros a los fines evaluativos o una versión full que demora más por la cantidad de registros que trae de las APIs.
