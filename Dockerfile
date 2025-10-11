# Use official Python slim image
FROM python:3.11-slim

WORKDIR /app

# Copy application code
COPY . /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    fontconfig \
    libfreetype6 \
    libjpeg62-turbo \
    libxrender1 \
    xfonts-75dpi \
    xfonts-base \
    && rm -rf /var/lib/apt/lists/*

# Download wkhtmltopdf .deb and install
RUN curl -LO https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox_0.12.6-1.buster_amd64.deb \
    && dpkg -i wkhtmltox_0.12.6-1.buster_amd64.deb \
    && apt-get install -f -y \
    && rm wkhtmltox_0.12.6-1.buster_amd64.deb

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run app
CMD ["python", "main.py"]
