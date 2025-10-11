# Use Python 3.11
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy all files into container
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables (optional defaults)
ENV SMTP_SERVER=smtp.gmail.com
ENV SMTP_PORT=587

# Command to run the Python script
CMD ["python", "main.py"]
