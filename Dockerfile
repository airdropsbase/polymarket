FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Prefer full Fly requirements when present (live trading + gunicorn)
COPY requirements-fly.txt requirements.txt ./
RUN pip install --upgrade pip setuptools wheel \
    && if [ -f requirements-fly.txt ]; then pip install -r requirements-fly.txt; \
       else pip install -r requirements.txt; fi

RUN mkdir -p /app/data
COPY . .

EXPOSE 8080

# Single worker — APScheduler must not run in multiple processes
CMD ["gunicorn", "app.main:app", \
     "-k", "uvicorn.workers.UvicornWorker", \
     "-b", "0.0.0.0:8080", \
     "--workers", "1", \
     "--timeout", "120", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]
