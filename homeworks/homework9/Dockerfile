FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY kafka_consumer.py /app/kafka_consumer.py

ENV PYTHONUNBUFFERED=1
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV TOPIC=tweets
ENV GROUP_ID=hw9_tweet_consumer_group
ENV OUTPUT_DIR=/output
ENV AUTO_OFFSET_RESET=earliest

CMD ["python", "/app/kafka_consumer.py"]
