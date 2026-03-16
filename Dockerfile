FROM ubuntu:latest
LABEL authors="sergi"

FROM python:3.12-slim

WORKDIR /logger_app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]