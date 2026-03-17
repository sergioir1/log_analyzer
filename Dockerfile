FROM python:3.12-slim

WORKDIR /logger_app

COPY requirements.txt .

RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
