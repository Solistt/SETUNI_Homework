import pandas as pd
import mysql.connector
import uuid
import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class AdTechETL:
    def __init__(self):
        self.config = {
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_ROOT_PASSWORD', 'root_password'),
            'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
            'database': os.getenv('MYSQL_DATABASE', 'adtech_db'),
            'autocommit': True  # Essential for large 1M+ row loads to prevent lock timeouts
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish connection to MySQL."""
        self.conn = mysql.connector.connect(**self.config)
        self.cursor = self.conn.cursor()

    def load_source_data(self, limit=1000000):
        """Read CSV files into DataFrames."""
        print(f"--> [1/5] Loading source files (limit={limit} rows)...")
        # Ensure these paths are correct for your local environment
        self.u_df = pd.read_csv('/Users/soloviov/adtech_project/users.csv')
        self.c_df = pd.read_csv('/Users/soloviov/adtech_project/campaigns.csv')
        self.e_df = pd.read_csv('/Users/soloviov/adtech_project/ad_events_header_updated.csv', 
                                nrows=limit, low_memory=False)

    def clean_data(self):
        """Clean NaNs and format binary/date columns."""
        print("--> [2/5] Cleaning data and formatting types...")
        self.u_df = self.u_df.where(pd.notnull(self.u_df), None)
        self.c_df = self.c_df.where(pd.notnull(self.c_df), None)
        self.e_df = self.e_df.where(pd.notnull(self.e_df), None)

        # Convert UUID strings to binary bytes for optimal storage and indexing
        self.e_df['EventID_bin'] = self.e_df['EventID'].apply(lambda x: uuid.UUID(x).bytes if x else None)
        
        # Format timestamps for MySQL compatibility
        self.e_df['ClickTimestamp'] = pd.to_datetime(self.e_df['ClickTimestamp'], errors='coerce')\
            .dt.strftime('%Y-%m-%d %H:%M:%S').where(pd.notnull(self.e_df['ClickTimestamp']), None)

    def load_metadata(self):
        """Insert users, advertisers, and campaigns using staging for relational mapping."""
        print("--> [3/5] Loading Metadata (Users, Advertisers, Campaigns)...")
        
        # Load Users
        self.cursor.executemany(
            "INSERT IGNORE INTO users (user_id, age, gender, location, interests, signup_date) VALUES (%s,%s,%s,%s,%s,%s)", 
            self.u_df.values.tolist()
        )
        
        # Load Advertisers
        adv_names = self.c_df[['AdvertiserName']].drop_duplicates().values.tolist()
        self.cursor.executemany("INSERT IGNORE INTO advertisers (name) VALUES (%s)", adv_names)

        # Load Campaigns via staging table to map advertiser names to advertiser_ids
        self.cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_c")
        self.cursor.execute("""
            CREATE TEMPORARY TABLE tmp_c (
                id INT, adv VARCHAR(255), name VARCHAR(255), sd DATE, ed DATE, 
                tc TEXT, sz VARCHAR(50), bg DECIMAL(15,2), rb DECIMAL(15,2)
            )
        """)
        self.cursor.executemany("INSERT INTO tmp_c VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", self.c_df.values.tolist())
        
        self.cursor.execute("""
            INSERT IGNORE INTO campaigns (campaign_id, advertiser_id, name, start_date, end_date, targeting_criteria, ad_slot_size, budget, remaining_budget) 
            SELECT t.id, a.advertiser_id, t.name, t.sd, t.ed, t.tc, t.sz, t.bg, t.rb 
            FROM tmp_c t 
            JOIN advertisers a ON t.adv = a.name
        """)

        # Fix: MySQL does not support IF NOT EXISTS for indexes. Handling via Exception.
        try:
            self.cursor.execute("ALTER TABLE campaigns ADD INDEX idx_camp_name (name)")
        except mysql.connector.Error as err:
            if err.errno == 1061: # Index already exists
                pass 
            else:
                raise

    def stage_impressions(self):
        """Load massive event data into temporary staging table with batching."""
        print("--> [4/5] Staging Impressions (1M records)...")
        self.cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_e")
        self.cursor.execute("""
            CREATE TEMPORARY TABLE tmp_e (
                eid BINARY(16), adv VARCHAR(255), cname VARCHAR(255), csd DATE, ced DATE, 
                ctc TEXT, cti TEXT, ctcry TEXT, asz VARCHAR(50), uid INT, dev VARCHAR(50), 
                loc VARCHAR(100), ts DATETIME, bid FLOAT, cost FLOAT, clicked BOOLEAN, 
                cts DATETIME, rev FLOAT, bud FLOAT, rbud FLOAT
            )
        """)

        cols = ['EventID_bin', 'AdvertiserName', 'CampaignName', 'CampaignStartDate', 'CampaignEndDate', 
                'CampaignTargetingCriteria', 'CampaignTargetingInterest', 'CampaignTargetingCountry', 
                'AdSlotSize', 'UserID', 'Device', 'Location', 'Timestamp', 'BidAmount', 'AdCost', 
                'WasClicked', 'ClickTimestamp', 'AdRevenue', 'Budget', 'RemainingBudget']
        
        event_list = self.e_df[cols].values.tolist()
        
        batch_size = 5000
        for i in range(0, len(event_list), batch_size):
            self.cursor.executemany("INSERT INTO tmp_e VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
                                    event_list[i:i+batch_size])
            if i % 100000 == 0:
                print(f"    Staged {i} records...")

        # Create index on temporary table to speed up the join in the final transfer
        self.cursor.execute("ALTER TABLE tmp_e ADD INDEX idx_tmp_cname (cname)")

    def transfer_to_facts(self):
        """Perform the final high-performance join and data transfer."""
        print("--> [5/5] Transferring to Fact Tables...")
        
        # Move to Impressions - Joining on the indexed 'name' column
        self.cursor.execute("""
            INSERT IGNORE INTO impressions (impression_id, campaign_id, user_id, device, location, timestamp, bid_amount, cost_paid) 
            SELECT e.eid, c.campaign_id, e.uid, e.dev, e.loc, e.ts, e.bid, e.cost 
            FROM tmp_e e 
            JOIN campaigns c ON e.cname = c.name
        """)
        
        # Move to Clicks
        self.cursor.execute("""
            INSERT IGNORE INTO clicks (impression_id, click_timestamp, revenue_generated) 
            SELECT eid, cts, rev FROM tmp_e WHERE clicked = 1
        """)
        print("--> Data transfer complete.")

    def run(self):
        try:
            self.connect()
            self.load_source_data()
            self.clean_data()
            self.load_metadata()
            self.stage_impressions()
            self.transfer_to_facts()
            print("\nETL Process finished successfully!")
        except Exception as e:
            print(f"\nCRITICAL ERROR: {e}")
        finally:
            if self.cursor: self.cursor.close()
            if self.conn: self.conn.close()

if __name__ == "__main__":
    etl = AdTechETL()
    etl.run()