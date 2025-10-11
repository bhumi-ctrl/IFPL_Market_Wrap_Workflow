# Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . /app

# Install wkhtmltopdf for Linux PDF conversion
RUN apt-get update && apt-get install -y wkhtmltopdf && apt-get clean

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
