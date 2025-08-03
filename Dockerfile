FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libfreetype6-dev \
    libpng-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY data/ ./data/

CMD ["python", "src/bot.py"]