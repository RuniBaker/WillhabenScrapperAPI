# Use Python 3.11 (more stable with Playwright)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and its browsers with dependencies
RUN playwright install --with-deps chromium

# Copy application code
COPY app.py .

# Expose port (Railway will set the PORT env variable)
EXPOSE 8080

# Run the application
CMD gunicorn app:app --bind 0.0.0.0:$PORT --timeout 120 --workers 1
```

## Step 2: Update requirements.txt

Make sure your `requirements.txt` contains:
```
flask==3.0.0
gunicorn==21.2.0
playwright==1.40.0
```

## Step 3: Optional - Create .dockerignore

Create a `.dockerignore` file to keep the build clean:
```
__pycache__
*.pyc
.git
.env
venv/
*.log