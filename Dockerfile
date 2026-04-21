FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Cloud Run expects the app to listen on the port defined by the PORT env var
ENV PORT 8080
CMD ["python", "main_v2.py"]

