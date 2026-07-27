FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim

COPY --from=builder /install /usr/local

RUN groupadd -r bridge && useradd -r -g bridge -d /home/bridge bridge

WORKDIR /app
COPY --chown=bridge:bridge . .

RUN mkdir -p /home/bridge/.ilink-bridge/auth /home/bridge/.ilink-bridge/logs \
    && chown -R bridge:bridge /home/bridge/.ilink-bridge

USER bridge

EXPOSE 8765

ENTRYPOINT ["python", "main.py"]
