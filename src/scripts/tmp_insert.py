from src.config import Config
import mysql.connector
import uuid
from datetime import datetime, timedelta


def main():
    cfg = Config.load_from_env()
    conn = mysql.connector.connect(**cfg.get_mysql_config())
    cur = conn.cursor()

    cur.execute("INSERT IGNORE INTO advertisers (name) VALUES (%s)", ("TestAdvertiser",))
    conn.commit()
    cur.execute("SELECT advertiser_id FROM advertisers WHERE name=%s", ("TestAdvertiser",))
    adv_id = cur.fetchone()[0]

    # Campaign
    campaign_id = 1
    cur.execute(
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
    cur.execute(
        "INSERT IGNORE INTO users (user_id, age, gender, location, interests, signup_date) VALUES (%s,%s,%s,%s,%s,%s)",
        (user_id, 30, 'Male', 'TestCity', 'tech,music', datetime.utcnow().date()),
    )
    conn.commit()

    # Impressions
    imp1 = uuid.uuid4().bytes
    imp2 = uuid.uuid4().bytes
    now = datetime.utcnow()

    impressions = [
        (imp1, campaign_id, user_id, 'Mobile', 'TestCity', now.strftime('%Y-%m-%d %H:%M:%S'), 0.5, 0.20),
        (imp2, campaign_id, user_id, 'Desktop', 'TestCity', (now + timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S'), 0.3, 0.10),
    ]
    cur.executemany(
        "INSERT IGNORE INTO impressions (impression_id, campaign_id, user_id, device, location, timestamp, bid_amount, cost_paid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        impressions,
    )
    conn.commit()

    # Click on first impression
    cur.execute(
        "INSERT IGNORE INTO clicks (impression_id, click_timestamp, revenue_generated) VALUES (%s,%s,%s)",
        (imp1, (now + timedelta(seconds=5)).strftime('%Y-%m-%d %H:%M:%S'), 1.00),
    )
    conn.commit()

    cur.close()
    conn.close()
    print('Inserted sample data')


if __name__ == '__main__':
    main()
