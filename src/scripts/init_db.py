"""Initialize MySQL schema and insert minimal sample data for testing.

This script reads database connection details from the root `.env` and:
 - runs the schema SQL to create tables
 - inserts a small set of sample advertisers, campaigns, users, impressions, and clicks

Run after MySQL is available and listening on the host configured in `.env`.
"""
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
import mysql.connector


BASE = Path(__file__).resolve().parents[2]
SCHEMA_SQL = BASE / 'homeworks' / 'hw2_mysql' / 'schema.sql'


def load_env():
    dotenv_path = BASE / '.env'
    if dotenv_path.exists():
        load_dotenv(dotenv_path)


def get_mysql_conn():
    cfg = {
        'user': os.getenv('MYSQL_USER', 'root'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
        'port': int(os.getenv('MYSQL_PORT', 3306)),
        'database': os.getenv('MYSQL_DATABASE', 'adtech_db'),
        'autocommit': True,
    }
    return mysql.connector.connect(**cfg)


def run_schema(conn):
    with open(SCHEMA_SQL, 'r', encoding='utf-8') as f:
        sql = f.read()

    cursor = conn.cursor()
    # Execute multi-statement SQL
    for result in cursor.execute(sql, multi=True):
        pass
    cursor.close()


def insert_sample_data(conn):
    cursor = conn.cursor()

    # Advertiser
    cursor.execute("INSERT IGNORE INTO advertisers (name) VALUES (%s)", ("TestAdvertiser",))
    conn.commit()
    cursor.execute("SELECT advertiser_id FROM advertisers WHERE name=%s", ("TestAdvertiser",))
    adv_id = cursor.fetchone()[0]

    # Campaign
    campaign_id = 1
    cursor.execute(
        "INSERT IGNORE INTO campaigns (campaign_id, advertiser_id, name, start_date, end_date, targeting_criteria, ad_slot_size, budget, remaining_budget) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            campaign_id,
            adv_id,
            'Test Campaign',
            (datetime.utcnow() - timedelta(days=7)).date(),
            (datetime.utcnow() + timedelta(days=30)).date(),
            'Tech',
            '300x250',
            1000.00,
            800.00,
        ),
    )
    conn.commit()

    # User
    user_id = 1000
    cursor.execute(
        "INSERT IGNORE INTO users (user_id, age, gender, location, interests, signup_date) VALUES (%s,%s,%s,%s,%s,%s)",
        (user_id, 30, 'Male', 'TestCity', 'tech,music', datetime.utcnow().date()),
    )
    conn.commit()

    # Impressions and clicks (use UUID bytes for BINARY(16))
    imp_ids = [uuid.uuid4().bytes, uuid.uuid4().bytes]
    now = datetime.utcnow()
    impressions = [
        (imp_ids[0], campaign_id, user_id, 'Mobile', 'TestCity', now.strftime('%Y-%m-%d %H:%M:%S'), 0.5, 0.20),
        (imp_ids[1], campaign_id, user_id, 'Desktop', 'TestCity', (now + timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S'), 0.3, 0.10),
    ]

    cursor.executemany(
        "INSERT IGNORE INTO impressions (impression_id, campaign_id, user_id, device, location, timestamp, bid_amount, cost_paid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        impressions,
    )
    conn.commit()

    # Click on first impression
    cursor.execute(
        "INSERT IGNORE INTO clicks (impression_id, click_timestamp, revenue_generated) VALUES (%s,%s,%s)",
        (imp_ids[0], (now + timedelta(seconds=5)).strftime('%Y-%m-%d %H:%M:%S'), 1.00),
    )
    conn.commit()

    cursor.close()

    print("Inserted sample advertiser_id=%s campaign_id=%s user_id=%s" % (adv_id, campaign_id, user_id))


def main():
    load_env()
    conn = get_mysql_conn()
    try:
        run_schema(conn)
        insert_sample_data(conn)
        print("Database initialized.")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
