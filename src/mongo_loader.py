import mysql.connector
from pymongo import MongoClient
import uuid
import datetime
import os
from collections import defaultdict

class MongoDataLoader:
    """
    ETL class to extract data from MySQL and load it into MongoDB
    using a document-oriented schema optimized for engagement history.
    """
    def __init__(self, mysql_config, mongo_uri, mongo_db_name):
        self.mysql_config = mysql_config
        self.mongo_uri = mongo_uri
        self.mongo_db_name = mongo_db_name
        self.mysql_conn = None
        self.mongo_client = None

    def connect(self):
        print("Connecting to MySQL and MongoDB...")
        try:
            self.mysql_conn = mysql.connector.connect(**self.mysql_config)
            self.mongo_client = MongoClient(self.mongo_uri)
            self.mongo_db = self.mongo_client[self.mongo_db_name]
            self.users_col = self.mongo_db['users']
            
            # Create indexes for the target collections
            self.users_col.create_index("sessions.impressions.campaign.campaign_id")
            self.users_col.create_index("sessions.impressions.campaign.advertiser_name")
            self.users_col.create_index("sessions.impressions.category")
            
            print("Successfully connected to both databases.")
        except Exception as e:
            print(f"Error connecting to databases: {e}")
            raise

    def close(self):
        if self.mysql_conn:
            self.mysql_conn.close()
        if self.mongo_client:
            self.mongo_client.close()

    def fetch_advertisers(self):
        cursor = self.mysql_conn.cursor(dictionary=True)
        cursor.execute("SELECT advertiser_id, name FROM advertisers")
        return {row['advertiser_id']: row['name'] for row in cursor.fetchall()}

    def fetch_campaigns(self):
        cursor = self.mysql_conn.cursor(dictionary=True)
        cursor.execute("SELECT campaign_id, advertiser_id, name, targeting_criteria FROM campaigns")
        return {row['campaign_id']: row for row in cursor.fetchall()}

    def load_data(self, batch_size=1000):
        print("Extracting reference data (advertisers, campaigns)...")
        advertisers = self.fetch_advertisers()
        campaigns = self.fetch_campaigns()

        cursor = self.mysql_conn.cursor(dictionary=True)
        
        # Get users
        cursor.execute("SELECT user_id, age, gender, location, interests, signup_date FROM users")
        users = cursor.fetchall()
        
        print(f"Found {len(users)} users. Beginning extraction and loading to MongoDB...")
        
        # Fetch all clicks into memory (assuming dataset fits, otherwise we batch by user_id)
        print("Fetching clicks...")
        click_cur = self.mysql_conn.cursor(dictionary=True)
        click_cur.execute("SELECT impression_id, click_timestamp, revenue_generated FROM clicks")
        clicks = {uuid.UUID(bytes=row['impression_id']).hex: row for row in click_cur.fetchall()}
        click_cur.close()

        # Iterate users in batches
        for i in range(0, len(users), batch_size):
            user_batch = users[i:i+batch_size]
            user_ids = [str(u['user_id']) for u in user_batch]
            
            if not user_ids:
                break
                
            in_clause = ",".join(user_ids)
            imp_query = f"""
                SELECT impression_id, campaign_id, user_id, device, timestamp, bid_amount
                FROM impressions
                WHERE user_id IN ({in_clause})
                ORDER BY timestamp ASC
            """
            
            imp_cur = self.mysql_conn.cursor(dictionary=True)
            imp_cur.execute(imp_query)
            impressions = imp_cur.fetchall()
            imp_cur.close()

            # Group impressions by user
            user_impressions = defaultdict(list)
            for imp in impressions:
                user_impressions[imp['user_id']].append(imp)

            mongo_documents = []
            
            for user in user_batch:
                uid = user['user_id']
                interests = str(user['interests']).split(',') if user.get('interests') else []
                
                # Group user's impressions into sessions (e.g., grouped by Day)
                # In real scenario, a session is defined by inactivity, here we use Date for simplicity
                sessions_map = defaultdict(list)
                
                for imp in user_impressions.get(uid, []):
                    imp_id_hex = uuid.UUID(bytes=imp['impression_id']).hex
                    imp_date = str(imp['timestamp'].date()) if imp['timestamp'] else 'unknown'
                    
                    camp = campaigns.get(imp['campaign_id'], {})
                    adv_name = advertisers.get(camp.get('advertiser_id'), 'Unknown')
                    
                    # Target category logic (extract from targeting criteria or interest)
                    category = camp.get('targeting_criteria', 'General')
                    
                    impression_doc = {
                        "impression_id": imp_id_hex,
                        "timestamp": imp['timestamp'],
                        "device": imp['device'],
                        "bid_amount": float(imp['bid_amount']) if imp['bid_amount'] is not None else None,
                        "campaign": {
                            "campaign_id": imp['campaign_id'],
                            "name": camp.get('name'),
                            "advertiser_name": adv_name
                        },
                        "category": category
                    }
                    
                    if imp_id_hex in clicks:
                        click = clicks[imp_id_hex]
                        impression_doc["click"] = {
                            "click_timestamp": click['click_timestamp'],
                            "revenue_generated": float(click['revenue_generated']) if click['revenue_generated'] is not None else None
                        }
                    
                    sessions_map[imp_date].append(impression_doc)

                sessions = []
                for s_date, s_imps in sessions_map.items():
                    sessions.append({
                        "session_date": s_date,
                        "session_start": min(i["timestamp"] for i in s_imps),
                        "impressions": s_imps
                    })
                
                # Sort sessions by start date
                sessions.sort(key=lambda x: x['session_start'])

                mongo_doc = {
                    "_id": uid,
                    "demographics": {
                        "age": user['age'],
                        "gender": user['gender'],
                        "location": user['location'],
                        "interests": [i.strip() for i in interests]
                    },
                    "signup_date": datetime.datetime.combine(user['signup_date'], datetime.time()) if user['signup_date'] else None,
                    "sessions": sessions
                }
                mongo_documents.append(mongo_doc)

            if mongo_documents:
                self.users_col.insert_many(mongo_documents)
            
            print(f"Loaded {i + len(user_batch)} / {len(users)} users to MongoDB.")

if __name__ == "__main__":
    mysql_config = {
        'user': 'root',
        'password': 'root_password',
        'host': '127.0.0.1',
        'database': 'adtech_db',
    }
    mongo_uri = "mongodb://root:root_password@127.0.0.1:27017/?authSource=admin"
    mongo_db_name = "adtech_nosql"
    
    loader = MongoDataLoader(mysql_config, mongo_uri, mongo_db_name)
    try:
        loader.connect()
        # Drop collection for clean reload
        loader.users_col.drop()
        loader.load_data(batch_size=5000)
        print("ETL to MongoDB completed successfully.")
    except Exception as e:
        print(f"ETL failed: {e}")
    finally:
        loader.close()