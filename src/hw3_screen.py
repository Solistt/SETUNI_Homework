import os
from dotenv import load_dotenv
import mysql.connector
from pymongo import MongoClient

# This tells the script to look one level up from 'src' for the .env file
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)

def print_validation_summary():
    # Debugging: Check if variables are actually loading
    if not os.getenv('MYSQL_ROOT_PASSWORD'):
        print("!!! ERROR: .env file not found or empty at:", dotenv_path)
        return

    print("="*50)
    print("   ADTECH SYSTEM VALIDATION SUMMARY")
    print("="*50)

    # 1. Check MySQL
    try:
        db = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', '127.0.0.1'),
            user=os.getenv('MYSQL_USER', 'root'),
            password=os.getenv('MYSQL_ROOT_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE')
        )
        cursor = db.cursor()
        cursor.execute("SELECT count(*) FROM impressions")
        imp_count = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM clicks")
        clk_count = cursor.fetchone()[0]
        print(f"[MySQL]   Impressions: {imp_count:,}")
        print(f"[MySQL]   Clicks:      {clk_count:,}")
        db.close()
    except Exception as e:
        print(f"[MySQL]   Error: {e}")

    # 2. Check MongoDB
    try:
        host = os.getenv('MONGO_HOST', '127.0.0.1')
        user = os.getenv('MONGO_USER', 'root')
        pw = os.getenv('MONGO_PASSWORD', 'root_password')
        db_name = os.getenv('MONGO_DB', 'adtech_nosql')

        client = MongoClient(f"mongodb://{user}:{pw}@{host}:27017/?authSource=admin")
        
        # Verify the DB name is not None before using it
        if not db_name:
            raise ValueError("MONGO_DB variable is missing in .env")

        users_col = client[db_name]['users']
        user_count = users_col.count_documents({})
        engaged_users = users_col.count_documents({"sessions.impressions.click": {"$exists": True}})
        
        print(f"[MongoDB] Total User Documents: {user_count:,}")
        print(f"[MongoDB] Users with Engagement: {engaged_users:,}")
        client.close()
    except Exception as e:
        print(f"[MongoDB] Error: {e}")

    print("="*50)
    print("   STATUS: CHECK COMPLETE")
    print("="*50)

if __name__ == "__main__":
    print_validation_summary()