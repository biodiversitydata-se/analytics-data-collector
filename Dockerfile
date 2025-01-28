FROM python:3.13-slim

WORKDIR /app
COPY collector.py /app
COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "collector.py"]
