# Dockerfile
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# Install PostgreSQL client
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create a non-root user and set permissions
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app

# Important: Set PLAYWRIGHT_BROWSERS_PATH for the non-root user
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 5000

# Run with gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--threads", "2", "--timeout", "120", "app:app"]