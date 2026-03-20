# Backend Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies + Node.js (for LiteParse)
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    libjpeg-dev \
    zlib1g-dev \
    tesseract-ocr \
    tesseract-ocr-eng \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install LiteParse globally
RUN npm install -g @llamaindex/liteparse

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create downloads directory
RUN mkdir -p /app/downloads /app/downloads/bpi

# Expose port
EXPOSE 8001

# Run the application
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8001"]
