FROM python:3.13-slim
ENV TZ=Europe/Stockholm

WORKDIR /app
COPY src/*.py /app
COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
