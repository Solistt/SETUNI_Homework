import os
import json
import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables for security and portability
load_dotenv()

class AdTechMongoQueries:
    """
    Production-ready class to execute AdTech business intelligence queries on MongoDB.
    Optimized for high-velocity user engagement data and nested documents.
    """
    def __init__(self):
        # Configuration from .env file
        user = os.getenv('MONGO_USER', 'root')
        password = os.getenv('MONGO_PASSWORD', 'root_password')
        host = os.getenv('MONGO_HOST', '127.0.0.1')
        db_name = os.getenv('MONGO_DB', 'adtech_nosql')

        self.mongo_uri = f"mongodb://{user}:{password}@{host}:27017/?authSource=admin"
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[db_name]
        self.users = self.db['users']
        
        # Path adjustment: Navigate from /src up to root, then into /output/hw3_reports
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.output_dir = os.path.join(base_path, "output", "hw3_reports")
        os.makedirs(self.output_dir, exist_ok=True)

    def _save_to_json(self, data, filename):
        """Helper to handle datetime serialization and save results to JSON."""
        filepath = os.path.join(self.output_dir, filename)
        
        def default_serializer(obj):
            if isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.isoformat()
            return str(obj)
            
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, default=default_serializer)
        print(f"--> [SUCCESS] Report generated: {filepath}")

    def q1_get_user_interaction_history(self, user_id):
        """Task 1: Retrieve all ad interactions for a specific user."""
        print(f"--> Running Task 1: Fetching history for User ID {user_id}...")
        projection = {
            "demographics": 1, 
            "sessions.session_date": 1,
            "sessions.impressions": 1
        }
        result = self.users.find_one({"_id": user_id}, projection)
        self._save_to_json(result, 'task1_user_interaction_history.json')

    def q2_get_multi_session_engagement(self, user_id):
        """Task 2: Retrieve the user’s last 5 ad sessions."""
        print(f"--> Running Task 2: Fetching last 5 sessions for User ID {user_id}...")
        projection = {"sessions": {"$slice": -5}}
        result = self.users.find_one({"_id": user_id}, projection)
        self._save_to_json(result, 'task2_multi_session_engagement.json')

    def q3_analyze_hourly_performance(self, advertiser_name):
        """Task 3: Aggregate clicks per hour for a specific advertiser."""
        print(f"--> Running Task 3: Performance analysis for {advertiser_name}...")
        pipeline = [
            {"$unwind": "$sessions"},
            {"$unwind": "$sessions.impressions"},
            {"$match": {
                "sessions.impressions.campaign.advertiser_name": advertiser_name,
                "sessions.impressions.click": {"$exists": True}
            }},
            {"$group": {
                "_id": {
                    "campaign": "$sessions.impressions.campaign.name",
                    "hour": {"$hour": "$sessions.impressions.click.click_timestamp"}
                },
                "total_clicks": {"$sum": 1},
                "total_revenue": {"$sum": "$sessions.impressions.click.revenue_generated"}
            }},
            {"$sort": {"_id.hour": 1}}
        ]
        results = list(self.users.aggregate(pipeline))
        self._save_to_json(results, 'task3_hourly_performance.json')

    def q4_detect_ad_fatigue(self, threshold=2):
        """Task 4: Find users who saw the same ad multiple times without clicking."""
        print(f"--> Running Task 4: Ad Fatigue Detection (Threshold >= {threshold})...")
        pipeline = [
            {"$unwind": "$sessions"},
            {"$unwind": "$sessions.impressions"},
            {"$group": {
                "_id": {
                    "user_id": "$_id",
                    "campaign_id": "$sessions.impressions.campaign.campaign_id"
                },
                "campaign_name": {"$first": "$sessions.impressions.campaign.name"},
                "impressions_count": {"$sum": 1},
                "clicks_count": {"$sum": {"$cond": [{"$ifNull": ["$sessions.impressions.click", False]}, 1, 0]}}
            }},
            {"$match": {
                "impressions_count": {"$gte": threshold},
                "clicks_count": 0
            }},
            {"$limit": 50}
        ]
        results = list(self.users.aggregate(pipeline))
        self._save_to_json(results, 'task4_ad_fatigue_report.json')

    def q5_get_real_time_targeting_categories(self, user_id):
        """Task 5: Retrieve a user’s top 3 engaged ad categories based on clicks."""
        print(f"--> Running Task 5: Generating profile for User ID {user_id}...")
        pipeline = [
            {"$match": {"_id": user_id}},
            {"$unwind": "$sessions"},
            {"$unwind": "$sessions.impressions"},
            {"$match": {"sessions.impressions.click": {"$exists": True}}},
            {"$group": {
                "_id": "$sessions.impressions.category",
                "click_count": {"$sum": 1}
            }},
            {"$sort": {"click_count": -1}},
            {"$limit": 3}
        ]
        results = list(self.users.aggregate(pipeline))
        self._save_to_json(results, 'task5_real_time_targeting.json')

    def discover_and_run(self):
        """Dynamically identifies users with actual data to ensure reports are populated."""
        print("--> Scanning MongoDB for valid test subjects...")
        
        # Find a user with at least one click (for Task 5)
        active_subject = self.db.users.find_one(
            {"sessions.impressions.click": {"$exists": True}},
            {"_id": 1, "sessions.impressions.campaign.advertiser_name": 1}
        )

        if not active_subject:
            print("CRITICAL: No click data found. Ensure mongo_loader.py successfully matched clicks.")
            return

        uid = active_subject["_id"]
        # Safeguard if sessions list is empty or advertiser name missing
        try:
            adv_name = active_subject["sessions"][0]["impressions"][0]["campaign"]["advertiser_name"]
        except (KeyError, IndexError):
            adv_name = "Unknown"

        # Execute all tasks
        self.q1_get_user_interaction_history(uid)
        self.q2_get_multi_session_engagement(uid)
        self.q3_analyze_hourly_performance(adv_name)
        
        # Threshold set to 2 to ensure we catch results in smaller data samples
        self.q4_detect_ad_fatigue(threshold=2) 
        
        self.q5_get_real_time_targeting_categories(uid)

if __name__ == "__main__":
    queries = AdTechMongoQueries()
    queries.discover_and_run()
    print("\n--- All BI Reports Generated Successfully ---")