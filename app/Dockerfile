FROM python:3.11-slim

# --- 1. App version ---
ARG APP_VERSION
ENV APP_VERSION=${APP_VERSION}

RUN echo "Building bot version: $APP_VERSION"

# Set working directory
WORKDIR /app

# Install system dependencies (needed for pandas, numpy, mysql)
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Run the bot
CMD ["python", "main.py"]