FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pandaproxy.py dcr.py core.py app.py ./

USER 568:568
CMD ["python", "-u", "app.py"]
