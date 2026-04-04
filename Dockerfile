FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir python-dotenv==1.0.1 qrcode==8.2 pillow==11.3.0

CMD ["python", "main.py"]
