@"
FROM jupyter/pyspark-notebook:spark-3.5.0

# Directorio de trabajo (donde vas a montar/copiar tu app)
WORKDIR /home/jovyan/app

# Copiamos requirements primero (si no existe, igual no falla)
COPY requirements.txt ./requirements.txt
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Copiamos el código
COPY src ./src

# (opcional) configs/scripts extra
# COPY conf ./conf

# S3 para Spark (mismos paquetes que usás en compose)
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.696 pyspark-shell"

# Región por defecto (podés sobreescribirla por variable de entorno al correr)
ENV AWS_REGION=us-east-1

# Usuario por defecto de la imagen base (evita problemas de permisos)
USER \${NB_UID}

# Queda “vivo” y después ejecutás lo que quieras con docker compose exec
CMD ["bash","-lc","sleep infinity"]
"@ | Set-Content -Encoding UTF8 -NoNewline Dockerfile