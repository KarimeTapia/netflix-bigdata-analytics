FROM python:3.11-slim

# Instalar dependencias del sistema y Java para compatibilidad con PySpark/Hadoop
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# Copiar requerimientos e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del proyecto
COPY . .

# Comando por defecto: orquestador maestro
CMD ["python", "run_project.py"]
