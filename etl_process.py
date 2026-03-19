import pandas as pd
import mysql.connector
import uuid
import sys

config = {
    'user': 'root',
    'password': 'root_password',
    'host': '127.0.0.1',
    'database': 'adtech_db',
    'autocommit': False 
}

def run_etl():
    try:
        print("--> 1/5 грузимо файли (nrows=150000)...")
        u_df = pd.read_csv('users.csv')
        c_df = pd.read_csv('campaigns.csv')
      
        e_df = pd.read_csv('ad_events_header_updated.csv', nrows=150000, low_memory=False)

        print("--> 2/5 чистимо NaN...")
        u_df = u_df.where(pd.notnull(u_df), None)
        c_df = c_df.where(pd.notnull(c_df), None)
        e_df = e_df.where(pd.notnull(e_df), None)

        e_df['EventID_bin'] = e_df['EventID'].apply(lambda x: uuid.UUID(x).bytes if x else None)
        e_df['ClickTimestamp'] = pd.to_datetime(e_df['ClickTimestamp'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S').where(pd.notnull(e_df['ClickTimestamp']), None)

        print("--> 3/5 конект з MySQL...")
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        print("--> 4/5 Загрузка табель ")
        cursor.executemany("INSERT IGNORE INTO users VALUES (%s,%s,%s,%s,%s,%s)", u_df.values.tolist())
        
        adv_names = c_df[['AdvertiserName']].drop_duplicates().values.tolist()
        cursor.executemany("INSERT IGNORE INTO advertisers (name) VALUES (%s)", adv_names)

        cursor.execute("CREATE TEMPORARY TABLE tmp_c (id INT, adv VARCHAR(255), name VARCHAR(255), sd DATE, ed DATE, tc TEXT, sz VARCHAR(50), bg DECIMAL(15,2), rb DECIMAL(15,2))")
        cursor.executemany("INSERT INTO tmp_c VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", c_df.values.tolist())
        cursor.execute("INSERT IGNORE INTO campaigns SELECT t.id, a.advertiser_id, t.name, t.sd, t.ed, t.tc, t.sz, t.bg, t.rb FROM tmp_c t JOIN advertisers a ON t.adv = a.name")

        print("--> 5/5 загрузка Impressions ")
        cursor.execute("CREATE TEMPORARY TABLE tmp_e (eid BINARY(16), adv VARCHAR(255), cname VARCHAR(255), csd DATE, ced DATE, ctc TEXT, cti TEXT, ctcry TEXT, asz VARCHAR(50), uid INT, dev VARCHAR(50), loc VARCHAR(100), ts DATETIME, bid FLOAT, cost FLOAT, clicked BOOLEAN, cts DATETIME, rev FLOAT, bud FLOAT, rbud FLOAT)")

        cols = ['EventID_bin', 'AdvertiserName', 'CampaignName', 'CampaignStartDate', 'CampaignEndDate', 'CampaignTargetingCriteria', 'CampaignTargetingInterest', 'CampaignTargetingCountry', 'AdSlotSize', 'UserID', 'Device', 'Location', 'Timestamp', 'BidAmount', 'AdCost', 'WasClicked', 'ClickTimestamp', 'AdRevenue', 'Budget', 'RemainingBudget']
        event_list = e_df[cols].values.tolist()
        
        for i in range(0, len(event_list), 5000):
            cursor.executemany("INSERT INTO tmp_e VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", event_list[i:i+5000])
            if i % 25000 == 0: print(f"    Załadowano {i} rekordów do stagingu...")

       
        cursor.execute("ALTER TABLE tmp_e ADD INDEX (cname)")
        
        print("--> переносимо до табель")
        cursor.execute("INSERT IGNORE INTO impressions (impression_id, campaign_id, user_id, device, location, timestamp, bid_amount, cost_paid) SELECT e.eid, c.campaign_id, e.uid, e.dev, e.loc, e.ts, e.bid, e.cost FROM tmp_e e JOIN campaigns c ON e.cname = c.name")
        cursor.execute("INSERT IGNORE INTO clicks (impression_id, click_timestamp, revenue_generated) SELECT eid, cts, rev FROM tmp_e WHERE clicked = 1")

        conn.commit()
        print("ETL  закінчено")

    except Exception as e:
        print(f"BLAD: {e}")
        if 'conn' in locals(): conn.rollback()
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    run_etl()