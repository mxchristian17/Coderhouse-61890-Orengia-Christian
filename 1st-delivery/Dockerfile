# Usar una imagen base de Python
FROM python:3.9-slim

# Instalar PostgreSQL client (psql)
RUN apt-get update && apt-get install -y postgresql-client

# Establecer el directorio de trabajo en /app
WORKDIR /app

# Copiar los archivos necesarios al contenedor
COPY requirements.txt requirements.txt
COPY scripts/ scripts/
COPY credentials.env credentials.env

# Instalar las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Comando por defecto
CMD ["tail", "-f", "/dev/null"]
