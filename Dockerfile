FROM python:3.11-slim

WORKDIR /app
COPY src/main.py .
COPY src/requirements.txt .

RUN pip install --upgrade pip
RUN pip install --user --no-cache-dir -r requirements.txt
ENTRYPOINT ["python3", "main.py"]
