# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy code and requirements
COPY main.py requirements.txt template.docx /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y wkhtmltopdf

# Set entrypoint
CMD ["python", "main.py"]
