FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt && \
    pip list | grep playwright

RUN python -m playwright install --with-deps chromium

COPY app.py .

EXPOSE 8080

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--timeout", "120", "--workers", "1"]
```

But I suspect the real issue is your `requirements.txt`. Can you show me what's in it? It should be exactly:
```
flask==3.0.0
gunicorn==21.2.0
playwright==1.40.0