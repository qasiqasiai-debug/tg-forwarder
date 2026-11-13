FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts

RUN useradd -m bot && mkdir -p /data && chown bot /data
USER bot
ENV DATA_DIR=/data

CMD ["python", "-m", "app"]
