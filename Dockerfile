FROM python:3.11-slim

WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates wget
RUN mkdir -p /usr/local/share/ca-certificates/russian_trusted
RUN wget https://gu-st.ru/content/lending/russian_trusted_root_ca_pem.crt -O /usr/local/share/ca-certificates/russian_trusted/russian_trusted_root_ca_pem
RUN wget https://gu-st.ru/content/lending/russian_trusted_sub_ca_pem.crt -O /usr/local/share/ca-certificates/russian_trusted/russian_trusted_sub_ca_pem
RUN update-ca-certificates

ENV SSL_CERT_FILE=/usr/local/share/ca-certificates/russian_trusted/russian_trusted_root_ca_pem

COPY src/main.py .
COPY src/requirements.txt .
RUN pip install --upgrade pip
RUN pip install --user --no-cache-dir -r requirements.txt
ENTRYPOINT ["python3", "main.py"]
