FROM python:3.11-slim

# Create a non-root user
RUN useradd -m botuser

# --- 1. App version ---
ARG APP_VERSION
ENV APP_VERSION=${APP_VERSION}

RUN echo "Building bot version: $APP_VERSION"

# Set working directory
WORKDIR /app

# make debian to automatically fallback to non interactive package installation 
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies (needed for pandas, numpy, mysql)
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

#upgrade pip (optional)
RUN pip install --upgrade pip

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Switch to non-root user (optional but more secure)
USER botuser

# Run the bot
CMD ["python", "main.py"]