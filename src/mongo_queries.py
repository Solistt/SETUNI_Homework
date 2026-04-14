import os
import json
from datetime import datetime, timedelta, timezone
from bson import ObjectId

from .config import Config
from .connection import ConnectionFactory


class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, (datetime,)):
            return obj.isoformat()
        return super().default(obj)


class AdTechMongoQueries:
    """Class to execute AdTech BI queries on MongoDB with optimized pipelines.

    This class loads configuration from the environment (fail-fast) and
    avoids leaking credentials in logs.
    """

    def __init__(self, config: Config = None):
        # Load configuration (will raise if required values are missing)
        self.config = config or Config.load_from_env()
        self.conn_factory = ConnectionFactory(self.config)
        self.client = self.conn_factory.get_mongo_client()
        self.db = self.client[self.config.mongo_database]
        self.users = self.db['users']

        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.output_dir = os.path.join(base_path, 'output', 'hw3_reports')
        os.makedirs(self.output_dir, exist_ok=True)

    def _save_to_json(self, data, filename):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, cls=MongoJSONEncoder)
        print(f"--> [SUCCESS] Report generated: {filepath}")

    def q1_get_user_interaction_history(self, user_id):
        print(f"--> Running Task 1: Fetching history for User ID {user_id}...")
        projection = {"demographics": 1, "sessions": 1}
        result = self.users.find_one({"_id": user_id}, projection)
        self._save_to_json(result, 'task1_user_interaction_history.json')

    def q2_get_multi_session_engagement(self, user_id):
        print(f"--> Running Task 2: Fetching last 5 sessions for User ID {user_id}...")
        projection = {"sessions": {"$slice": -5}}
        result = self.users.find_one({"_id": user_id}, projection)
        self._save_to_json(result, 'task2_multi_session_engagement.json')

    def q3_analyze_hourly_performance(self, advertiser_name):
        print(f"--> Running Task 3: Performance analysis for {advertiser_name}...")

        now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        start_utc = now_utc - timedelta(hours=24)

        pipeline = [
            # Filter documents containing relevant impressions first
            {"$match": {
                "sessions.impressions.campaign.advertiser_name": advertiser_name,
                "sessions.impressions.timestamp": {"$gte": start_utc, "$lt": now_utc}
            }},
            {"$unwind": "$sessions"},
            {"$unwind": "$sessions.impressions"},
            {"$match": {
                "sessions.impressions.campaign.advertiser_name": advertiser_name,
                "sessions.impressions.timestamp": {"$gte": start_utc, "$lt": now_utc}
            }},
            {"$group": {
                "_id": {
                    "campaign": "$sessions.impressions.campaign.name",
                    "hour": {"$dateToString": {"format": "%Y-%m-%dT%H:00:00Z", "date": "$sessions.impressions.timestamp", "timezone": "UTC"}}
                },
                "total_clicks": {"$sum": {"$cond": [{"$ifNull": ["$sessions.impressions.click", False]}, 1, 0]}},
                "total_revenue": {"$sum": {"$ifNull": ["$sessions.impressions.click.revenue_generated", 0]}}
            }},
            {"$sort": {"_id.hour": 1}}
        ]

        results = list(self.users.aggregate(pipeline))
        self._save_to_json(results, 'task3_hourly_performance.json')

    def q4_detect_ad_fatigue(self, threshold=2):
        print(f"--> Running Task 4: Ad Fatigue Detection (Threshold >= {threshold})...")

        pipeline = [
            # Quick filter to remove documents without impressions
            {"$match": {"sessions.impressions.campaign.campaign_id": {"$exists": True}}},
            {"$unwind": "$sessions"},
            {"$unwind": "$sessions.impressions"},
            {"$group": {
                "_id": {"user_id": "$_id", "campaign_id": "$sessions.impressions.campaign.campaign_id"},
                "campaign_name": {"$first": "$sessions.impressions.campaign.name"},
                "impressions_count": {"$sum": 1},
                "clicks_count": {"$sum": {"$cond": [{"$ifNull": ["$sessions.impressions.click", False]}, 1, 0]}}
            }},
            {"$match": {"impressions_count": {"$gte": threshold}, "clicks_count": 0}},
            {"$limit": 50}
        ]

        results = list(self.users.aggregate(pipeline))
        self._save_to_json(results, 'task4_ad_fatigue_report.json')

    def q5_get_real_time_targeting_categories(self, user_id):
        print(f"--> Running Task 5: Generating profile for User ID {user_id}...")

        pipeline = [
            {"$match": {"_id": user_id}},
            {"$unwind": "$sessions"},
            {"$unwind": "$sessions.impressions"},
            {"$match": {"sessions.impressions.click": {"$exists": True}}},
            {"$group": {"_id": "$sessions.impressions.category", "click_count": {"$sum": 1}}},
            {"$sort": {"click_count": -1}},
            {"$limit": 3}
        ]

        results = list(self.users.aggregate(pipeline))
        self._save_to_json(results, 'task5_real_time_targeting.json')

    def discover_and_run(self):
        print("--> Scanning MongoDB for valid test subjects...")

        active_subject = self.db.users.find_one(
            {"sessions.impressions.click": {"$exists": True}},
            {"_id": 1, "sessions.impressions.campaign.advertiser_name": 1}
        )

        if not active_subject:
            print("CRITICAL: No click data found. Ensure mongo_loader.py successfully matched clicks.")
            return

        uid = active_subject["_id"]
        try:
            adv_name = active_subject["sessions"][0]["impressions"][0]["campaign"]["advertiser_name"]
        except (KeyError, IndexError):
            adv_name = "Unknown"

        self.q1_get_user_interaction_history(uid)
        self.q2_get_multi_session_engagement(uid)
        self.q3_analyze_hourly_performance(adv_name)
        self.q4_detect_ad_fatigue(threshold=2)
        self.q5_get_real_time_targeting_categories(uid)


if __name__ == '__main__':
    queries = AdTechMongoQueries()
    queries.discover_and_run()
    print('\n--- All BI Reports Generated Successfully ---')