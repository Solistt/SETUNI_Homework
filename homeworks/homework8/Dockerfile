FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY tweet_producer.py /app/tweet_producer.py

ENV PYTHONUNBUFFERED=1
ENV INPUT_FILE=/data/amazon_reviews.csv
ENV TOPIC=tweets
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV MIN_MSGS_PER_SEC=10
ENV MAX_MSGS_PER_SEC=15
ENV MAX_RUNTIME_SECONDS=300

CMD ["python", "/app/tweet_producer.py"]
