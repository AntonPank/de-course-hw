FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd --create-home --uid 1000 appuser
RUN mkdir -p /cache && chown appuser:appuser /cache
COPY app/gh_ingest.py .
USER appuser
ENTRYPOINT ["python", "/app/gh_ingest.py"]