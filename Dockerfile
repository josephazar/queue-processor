#FROM --platform=linux/amd64 python:3.10
FROM python:3.10

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    libodbc1 \
    gcc \
    g++ \
    curl \
    gnupg \
    procps \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Make the health check script executable
RUN chmod +x healthcheck.sh

# Create logs directory
RUN mkdir -p logs

RUN ls -la /app
RUN ls -la /app/logs
RUN touch /app/logs/test.log
RUN echo "Test" > /app/logs/test.log

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the processor
ENTRYPOINT ["python", "processor.py"]
