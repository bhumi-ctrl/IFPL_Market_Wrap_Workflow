FROM python:3.11-slim

WORKDIR /app
COPY . /app

# Install dependencies
RUN apt-get update && apt-get install -y wkhtmltopdf
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
