FROM python:3.9-slim

WORKDIR /app

COPY transcode.py requirements.txt ./
RUN python3 -mpip install -r requirements.txt && \
    apt-get update && apt-get install -y ffmpeg lame

CMD ["python3", "-u", "/app/transcode.py"]
