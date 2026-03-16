FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN apt-get update \
    && apt-get install --no-install-recommends -y yt-dlp ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["python", "-u", "app.py"]
