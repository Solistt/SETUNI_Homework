FROM python:3.10-slim

WORKDIR /app

# Install Python dependencies
COPY src/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the `src` package into /app/src so `src.api` is importable
COPY src /app/src

# Ensure the app root is on PYTHONPATH
ENV PYTHONPATH=/app

CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
