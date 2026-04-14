import json
import logging
import redis
from fastapi import FastAPI, Query, HTTPException

from .config import Config
from .connection import ConnectionFactory


logger = logging.getLogger(__name__)
app = FastAPI(title="AdTech Analytics API")

# Load configuration (fail-fast if required env vars are missing)
config = Config.load_from_env()
conn_factory = ConnectionFactory(config)


@app.on_event("startup")
def startup_event():
    # Initialize connections on startup using the connection factory
    app.state.conn_factory = conn_factory
    app.state.redis = redis.Redis(host=config.redis_host, port=config.redis_port, db=0, decode_responses=True)


def get_mysql_conn():
    return app.state.conn_factory.get_mysql_connection()


def get_users_collection():
    client = app.state.conn_factory.get_mongo_client()
    return client[config.mongo_database]['users']

@app.get("/campaign/{campaign_id}/performance")
def get_campaign_performance(campaign_id: int, use_cache: bool = Query(True)):
    cache_key = f"campaign:{campaign_id}:performance"
    
    if use_cache:
        cached_data = app.state.redis.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

    try:
        conn = get_mysql_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                COUNT(i.impression_id) as impressions,
                COUNT(c.click_id) as clicks,
                SUM(i.cost_paid) as ad_spend
            FROM impressions i
            LEFT JOIN clicks c ON i.impression_id = c.impression_id
            WHERE i.campaign_id = %s
        """
        cursor.execute(query, (campaign_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        impressions = result["impressions"] if result and result["impressions"] else 0
        clicks = result["clicks"] if result and result["clicks"] else 0
        ad_spend = float(result["ad_spend"]) if result and result["ad_spend"] else 0.0
        ctr = (clicks / impressions) if impressions > 0 else 0.0

        response_data = {
            "campaign_id": campaign_id,
            "impressions": impressions,
            "clicks": clicks,
            "ctr": ctr,
            "ad_spend": ad_spend
        }

        if use_cache:
            app.state.redis.setex(cache_key, 30, json.dumps(response_data))

        return response_data
    except Exception as e:
        logger.exception("Error fetching campaign performance")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/advertiser/{advertiser_id}/spending")
def get_advertiser_spending(advertiser_id: int, use_cache: bool = Query(True)):
    cache_key = f"advertiser:{advertiser_id}:spending"
    
    if use_cache:
        cached_data = app.state.redis.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

    try:
        conn = get_mysql_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT SUM(i.cost_paid) as total_ad_spend
            FROM impressions i
            JOIN campaigns c ON i.campaign_id = c.campaign_id
            WHERE c.advertiser_id = %s
        """
        cursor.execute(query, (advertiser_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        total_ad_spend = float(result["total_ad_spend"]) if result and result["total_ad_spend"] else 0.0

        response_data = {
            "advertiser_id": advertiser_id,
            "total_ad_spend": total_ad_spend
        }

        if use_cache:
            app.state.redis.setex(cache_key, 300, json.dumps(response_data))

        return response_data
    except Exception as e:
        logger.exception("Error fetching advertiser spending")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/user/{user_id}/engagements")
def get_user_engagements(user_id: int, use_cache: bool = Query(True)):
    cache_key = f"user:{user_id}:engagements"
    
    if use_cache:
        cached_data = app.state.redis.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

    try:
        users_col = get_users_collection()
        user = users_col.find_one({"_id": user_id})
        engagements = []
        if user:
            for session in user.get("sessions", []):
                for imp in session.get("impressions", []):
                    engagements.append({
                        "campaign_id": imp["campaign"].get("campaign_id"),
                        "campaign_name": imp["campaign"].get("name"),
                        "advertiser_name": imp["campaign"].get("advertiser_name"),
                        "timestamp": imp["timestamp"].isoformat() if hasattr(imp.get("timestamp"), "isoformat") else imp.get("timestamp"),
                        "category": imp.get("category", "General"),
                        "engagement_type": "click" if imp.get("click") else "impression"
                    })

        response_data = {"user_id": user_id, "engagements": engagements}

        if use_cache:
            app.state.redis.setex(cache_key, 60, json.dumps(response_data))

        return response_data
    except Exception as e:
        logger.exception("Error fetching user engagements")
        raise HTTPException(status_code=500, detail="Internal server error")
