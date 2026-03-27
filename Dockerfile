FROM python:3.11-slim

# Firebird client libs (para extraer de Firebird legacy)
RUN apt-get update && apt-get install -y --no-install-recommends \
    firebird3.0-utils \
    libfbclient2 \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# dbt necesita que el proyecto esté instalado
RUN pip install -e .

RUN mkdir -p /app/.dagster

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
