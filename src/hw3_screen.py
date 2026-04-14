import os
import logging

from .config import Config
from .connection import ConnectionFactory


logger = logging.getLogger(__name__)


def print_validation_summary():
    """Validate connections to MySQL and MongoDB using environment configuration.

    This function reads credentials from the root `.env` via Config and does
    not print sensitive values.
    """
    try:
        cfg = Config.load_from_env()
    except Exception as e:
        logger.error("Configuration error: %s", str(e))
        print("ERROR: Configuration missing. See example.env for required variables.")
        return

    factory = ConnectionFactory(cfg)

    print("=" * 50)
    print("   ADTECH SYSTEM VALIDATION SUMMARY")
    print("=" * 50)

    # 1. Check MySQL
    try:
        db = factory.get_mysql_connection()
        cursor = db.cursor()
        cursor.execute("SELECT count(*) FROM impressions")
        imp_count = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM clicks")
        clk_count = cursor.fetchone()[0]
        print(f"[MySQL]   Impressions: {imp_count:,}")
        print(f"[MySQL]   Clicks:      {clk_count:,}")
        db.close()
    except Exception as e:
        logger.exception("MySQL validation failed")
        print(f"[MySQL]   Error: {e}")

    # 2. Check MongoDB
    try:
        client = factory.get_mongo_client()
        db = client[cfg.mongo_database]
        users_col = db['users']

        user_count = users_col.count_documents({})
        engaged_users = users_col.count_documents({"sessions.impressions.click": {"$exists": True}})

        print(f"[MongoDB] Total User Documents: {user_count:,}")
        print(f"[MongoDB] Users with Engagement: {engaged_users:,}")
        client.close()
    except Exception as e:
        logger.exception("MongoDB validation failed")
        print(f"[MongoDB] Error: {e}")

    print("=" * 50)
    print("   STATUS: CHECK COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    print_validation_summary()