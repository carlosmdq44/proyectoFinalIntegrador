FROM jupyter/pyspark-notebook:spark-3.5.0

# Directorio de trabajo
WORKDIR /home/jovyan/app

# Instalar requirements si existen (no falla si no hay archivo)
COPY requirements.txt ./requirements.txt
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Copiar el código
COPY src ./src

# Paquetes para S3 en Spark
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.696 pyspark-shell"

# Región por defecto (se puede sobreescribir en runtime)
ENV AWS_REGION=us-east-1

# Usuario por defecto de la imagen base (NO escapa el $)
USER ${NB_UID}

# Mantener el contenedor vivo; luego ejecutás con docker compose exec
CMD ["bash","-lc","sleep infinity"]