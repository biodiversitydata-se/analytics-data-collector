FROM python:3.14-slim
ENV TZ=Europe/Stockholm

WORKDIR /app
COPY src/*.py /app
COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "main.py"]
