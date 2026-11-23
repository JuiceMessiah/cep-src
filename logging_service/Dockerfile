FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir flask==3.0.3
COPY app.py .
RUN mkdir -p /logs /XES

CMD ["python", "-u", "app.py"]
