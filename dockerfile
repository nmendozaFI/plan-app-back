FROM python:3.10-slim

# Instala dependencias del sistema (por ejemplo, para FFmpeg y PyAV)
RUN apt-get update && apt-get install -y \
      wget \
      xz-utils \
      ffmpeg \
      libavcodec-dev \
      libavformat-dev \
      libavdevice-dev \
      libavfilter-dev \
      libavutil-dev \
      libswresample-dev \
      libswscale-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Crea el entorno virtual
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copia el archivo de requerimientos e instala las dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación
COPY . .

# Expone el puerto de la aplicación
EXPOSE 8080

# Comando para iniciar la aplicación
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]