# Mock services image — FastAPI multiplexed by Host header.
# Build context: example root (so `mocks/` is reachable).
#   docker build -t agent-harness-platform-mock -f docker/mock.Dockerfile .
FROM python:3.11-slim

RUN pip install --no-cache-dir 'fastapi>=0.110' 'uvicorn[standard]>=0.30'

COPY mocks/server.py /app/server.py
WORKDIR /app

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "80"]
