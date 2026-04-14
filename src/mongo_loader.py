import uuid
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from pymongo import UpdateOne

from .config import Config
from .connection import ConnectionFactory


logger = logging.getLogger(__name__)


def _to_hex_id(raw):
    if raw is None:
        return None
    # If stored as binary(16) bytes
    if isinstance(raw, (bytes, bytearray)):
        try:
            return uuid.UUID(bytes=raw).hex
        except Exception:
            try:
                return raw.hex()
            except Exception:
                return str(raw)
    # If already a UUID string
    try:
        return str(raw)
    except Exception:
        return None


class MongoDataLoader:
    """ETL class to stream data from MySQL into MongoDB using batch and bulk operations."""

    def __init__(self, config: Config, connection_factory: ConnectionFactory = None):
        self.config = config
        self.connection_factory = connection_factory or ConnectionFactory(config)
        self.mysql_conn = None
        self.mongo_client = None
        self.users_col = None

    def connect(self):
        logger.info("Connecting to MySQL and MongoDB (secrets loaded from environment).")
        self.mysql_conn = self.connection_factory.get_mysql_connection()
        self.mongo_client = self.connection_factory.get_mongo_client()
        self.mongo_db = self.mongo_client[self.config.mongo_db]
        self.users_col = self.mongo_db['users']

        # Create helpful indexes for analytics
        self.users_col.create_index("sessions.impressions.campaign.campaign_id")
        self.users_col.create_index("sessions.impressions.campaign.advertiser_name")
        self.users_col.create_index("sessions.impressions.category")

    def close(self):
        if self.mysql_conn:
            try:
                self.mysql_conn.close()
            except Exception:
                pass
        if self.mongo_client:
            try:
                self.mongo_client.close()
            except Exception:
                pass

    def fetch_advertisers(self):
        cur = self.mysql_conn.cursor(dictionary=True)
        cur.execute("SELECT advertiser_id, name FROM advertisers")
        rows = cur.fetchall()
        cur.close()
        return {row['advertiser_id']: row['name'] for row in rows}

    def fetch_campaigns(self):
        cur = self.mysql_conn.cursor(dictionary=True)
        cur.execute("SELECT campaign_id, advertiser_id, name, targeting_criteria FROM campaigns")
        rows = cur.fetchall()
        cur.close()
        return {row['campaign_id']: row for row in rows}

    def _normalize_ts(self, ts):
        if ts is None:
            return None
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                return ts.replace(tzinfo=timezone.utc)
            return ts.astimezone(timezone.utc)
        return ts

    def load_data(self, drop_existing=False):
        logger.info("Begin ETL: extracting reference data and streaming users in batches.")
        advertisers = self.fetch_advertisers()
        campaigns = self.fetch_campaigns()

        if drop_existing:
            logger.info("Dropping existing users collection before reload.")
            try:
                self.users_col.drop()
            except Exception:
                pass

        batch_size = self.config.user_batch_size
        bulk_size = self.config.bulk_write_batch_size
        session_timeout = int(self.config.session_timeout_seconds)

        offset = 0
        total_loaded = 0

        while True:
            ucur = self.mysql_conn.cursor(dictionary=True)
            ucur.execute(
                "SELECT user_id, age, gender, location, interests, signup_date FROM users ORDER BY user_id LIMIT %s OFFSET %s",
                (batch_size, offset),
            )
            users = ucur.fetchall()
            ucur.close()

            if not users:
                break

            user_ids = [u['user_id'] for u in users]

            # Build parameter placeholders for the IN clause
            placeholders = ','.join(['%s'] * len(user_ids))
            imp_query = f'''
                SELECT i.impression_id, i.campaign_id, i.user_id, i.device, i.timestamp, i.bid_amount,
                       c.click_timestamp as click_timestamp, c.revenue_generated as revenue_generated
                FROM impressions i
                LEFT JOIN clicks c ON i.impression_id = c.impression_id
                WHERE i.user_id IN ({placeholders})
                ORDER BY i.user_id, i.timestamp ASC
            '''

            icur = self.mysql_conn.cursor(dictionary=True)
            icur.execute(imp_query, tuple(user_ids))
            impressions = icur.fetchall()
            icur.close()

            # Group impressions by user
            user_impressions = defaultdict(list)
            for imp in impressions:
                user_impressions[imp['user_id']].append(imp)

            # Prepare bulk upserts
            bulk_ops = []

            for user in users:
                uid = user['user_id']
                interests = (str(user.get('interests') or '')).split(',') if user.get('interests') else []

                sessions = []
                current_session = None
                last_ts = None

                for imp in user_impressions.get(uid, []):
                    imp_ts = self._normalize_ts(imp.get('timestamp'))
                    imp_id_hex = _to_hex_id(imp.get('impression_id'))

                    camp = campaigns.get(imp.get('campaign_id'), {})
                    adv_name = advertisers.get(camp.get('advertiser_id'), 'Unknown')
                    category = camp.get('targeting_criteria', 'General')

                    impression_doc = {
                        'impression_id': imp_id_hex,
                        'timestamp': imp_ts,
                        'device': imp.get('device'),
                        'bid_amount': float(imp['bid_amount']) if imp.get('bid_amount') is not None else None,
                        'campaign': {
                            'campaign_id': imp.get('campaign_id'),
                            'name': camp.get('name'),
                            'advertiser_name': adv_name,
                        },
                        'category': category,
                    }

                    if imp.get('click_timestamp'):
                        click_ts = self._normalize_ts(imp.get('click_timestamp'))
                        impression_doc['click'] = {
                            'click_timestamp': click_ts,
                            'revenue_generated': float(imp['revenue_generated']) if imp.get('revenue_generated') is not None else None,
                        }

                    # Sessionization by inactivity timeout
                    if current_session is None:
                        current_session = {
                            'session_start': imp_ts,
                            'impressions': [impression_doc],
                        }
                    else:
                        # If gap larger than timeout -> start new session
                        gap = (imp_ts - last_ts).total_seconds() if (imp_ts and last_ts) else 0
                        if gap > session_timeout:
                            sessions.append(current_session)
                            current_session = {'session_start': imp_ts, 'impressions': [impression_doc]}
                        else:
                            current_session['impressions'].append(impression_doc)

                    last_ts = imp_ts

                if current_session is not None:
                    sessions.append(current_session)

                # Sort sessions by start
                sessions.sort(key=lambda s: s.get('session_start') or datetime(1970, 1, 1, tzinfo=timezone.utc))

                mongo_doc = {
                    '_id': uid,
                    'demographics': {
                        'age': user.get('age'),
                        'gender': user.get('gender'),
                        'location': user.get('location'),
                        'interests': [i.strip() for i in interests if i and i.strip()],
                    },
                    'signup_date': (datetime.combine(user['signup_date'], datetime.min.time()).replace(tzinfo=timezone.utc)
                                    if user.get('signup_date') else None),
                    'sessions': sessions,
                }

                bulk_ops.append(UpdateOne({'_id': uid}, {'$set': mongo_doc}, upsert=True))

                # Flush bulk operations when reaching bulk_size
                if len(bulk_ops) >= bulk_size:
                    try:
                        self.users_col.bulk_write(bulk_ops, ordered=False)
                        total_loaded += len(bulk_ops)
                        logger.info(f"Flushed {len(bulk_ops)} user documents to MongoDB.")
                    except Exception as e:
                        logger.exception("Bulk write failed")
                    bulk_ops = []

            # Flush any remaining operations for this batch
            if bulk_ops:
                try:
                    self.users_col.bulk_write(bulk_ops, ordered=False)
                    total_loaded += len(bulk_ops)
                    logger.info(f"Flushed {len(bulk_ops)} user documents to MongoDB.")
                except Exception:
                    logger.exception("Final bulk write failed")

            offset += batch_size

        logger.info(f"ETL complete. Total user documents upserted: {total_loaded}")


if __name__ == '__main__':
    # Fail-fast configuration load
    try:
        cfg = Config.load_from_env()
    except Exception as e:
        logger.error("Configuration error: %s", str(e))
        raise

    loader = MongoDataLoader(cfg)
    try:
        loader.connect()
        loader.load_data(drop_existing=True)
        logger.info("ETL to MongoDB completed successfully.")
    except Exception:
        logger.exception("ETL failed")
    finally:
        loader.close()