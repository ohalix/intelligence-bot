FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONUNBUFFERED=1

# NOTE: Cloud Run is request-driven; long polling is not ideal.
# This image is provided for completeness if you later switch to webhook/HTTP trigger architecture.
CMD ["python", "main.py"]
