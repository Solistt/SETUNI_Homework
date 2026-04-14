# Homework 3: AdTech MongoDB Integration

This directory contains the solutions for Homework 3, extending the relational database from Homework 1 by introducing NoSQL storage (MongoDB) for user engagement data.

## 1. MongoDB Schema Design

The schema is designed to embrace a document-oriented approach. Because of the high velocity of ad interactions and the necessity for fast session-based lookups, all impressions and clicks are embedded inside a `users` collection. The data is grouped by user and nested into **sessions**.

### Schema Structure (`users` collection)
```json
{
  "_id": 1, // Matches MySQL user_id
  "demographics": {
    "age": 28,
    "gender": "Female",
    "location": "Warsaw",
    "interests": ["tech", "sports"]
  },
  "signup_date": "2024-01-01T00:00:00Z",
  "sessions": [
    {
      "session_date": "2024-03-01",
      "session_start": "2024-03-01T10:00:00Z",
      "impressions": [
        {
          "impression_id": "8a32d1f9-...",
          "timestamp": "2024-03-01T10:05:00Z",
          "device": "Mobile",
          "bid_amount": 0.5,
          "category": "Tech",
          "campaign": {
            "campaign_id": 10,
            "name": "Spring Promo",
            "advertiser_name": "TechCorp"
          },
          "click": { // Included only if the impression was clicked
            "click_timestamp": "2024-03-01T10:05:05Z",
            "revenue_generated": 1.2
          }
        }
      ]
    }
  ]
}
```
**Optimization Notes:** 
- Instead of using foreign keys (`user_id`, `campaign_id`) requiring complex joins, the user demographic data, session groupings, campaign names, and click interactions are pre-joined and embedded. 
- Fast retrieval queries only require indexing the target fields (e.g. `sessions.impressions.campaign.advertiser_name`).

---

## 2. MongoDB Queries

### Task 1: Querying User Ad Interaction History
```javascript
db.users.find(
    {"_id": 1}, 
    {"demographics": 1, "sessions.impressions": 1}
);
```

### Task 2: Tracking Multi-Session User Engagement
```javascript
db.users.find(
    {"_id": 1}, 
    {"sessions": {"$slice": -5}}
);
```

### Task 3: Time-Windowed Ad Performance Analysis
```javascript
db.users.aggregate([
  // Stage 1: Early filter using an index to reduce documents processed.
  // This stage dramatically improves performance by filtering before unwinding.
  {"$match": {
     "sessions.impressions.campaign.advertiser_name": "TechCorp",
     "sessions.impressions.click": {"$exists": true}
  }},
  // Stage 2: Unwind arrays to de-normalize documents for grouping.
  {"$unwind": "$sessions"},
  {"$unwind": "$sessions.impressions"},
  // Stage 3: Match again on the specific unwound impression to ensure accuracy.
  {"$match": {
     "sessions.impressions.campaign.advertiser_name": "TechCorp"
  }},
  // Stage 4: Group by campaign and time window to aggregate results.
  {"$group": {
     "_id": {
         "campaign_id": "$sessions.impressions.campaign.campaign_id",
         "campaign_name": "$sessions.impressions.campaign.name",
         "hour": {"$hour": "$sessions.impressions.click.click_timestamp"},
         "date": {
             "$dateToString": { "format": "%Y-%m-%d", "date": "$sessions.impressions.click.click_timestamp" }
         }
     },
     "total_clicks": {"$sum": 1},
     "total_revenue": {"$sum": "$sessions.impressions.click.revenue_generated"}
  }},
  // Stage 5: Sort the results for presentation.
  {"$sort": {"_id.date": -1, "_id.hour": -1}}
]);
```

### Task 4: Detecting Ad Fatigue
```javascript
db.users.aggregate([
  {"$unwind": "$sessions"},
  {"$unwind": "$sessions.impressions"},
  {"$group": {
     "_id": {
         "user_id": "$_id",
         "campaign_id": "$sessions.impressions.campaign.campaign_id"
     },
     "impressions_count": {"$sum": 1},
     "clicks_count": {
         "$sum": { "$cond": [{"$ifNull": ["$sessions.impressions.click", false]}, 1, 0] }
     }
  }},
  {"$match": {
     "impressions_count": {"$gte": 2},
     "clicks_count": 0
  }},
  {"$limit": 100}
]);
```

### Task 5: Real-Time Lookups for Ad Targeting
```javascript
db.users.aggregate([
  {"$match": {"_id": 1}},
  {"$unwind": "$sessions"},
  {"$unwind": "$sessions.impressions"},
  {"$match": {
     "sessions.impressions.click": {"$exists": true}
  }},
  {"$group": {
     "_id": "$sessions.impressions.category",
     "clicks": {"$sum": 1}
  }},
  {"$sort": {"clicks": -1}},
  {"$limit": 3}
]);
```

---

## 3. How to Run

1. **Start the Database Infrastructure:**
   ```bash
   cd SETUNI_Homework
   docker-compose up -d
   ```

2. **Run the ETL Loader (MySQL -> MongoDB):**
   ```bash
   cd src/homeworks/homework3
   python3 mongo_loader.py
   ```
   *Take a screenshot of the loader terminal output.*

3. **Run the Queries and Generate JSON insights:**
   ```bash
   python3 mongo_queries.py
   ```
   *Take a screenshot of the script output and verify the created files in `src/homeworks/homework3/output/` folder.*

4. **Verify MongoDB using MongoSH (Optional):**
  You can verify the data visually by connecting to the MongoDB container. For security, source your `.env` first and then use the variables rather than embedding secrets directly:
  ```bash
  source ../..../example.env  # or set your env vars from your local .env file
  docker exec -it adtech_mongo mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD"
  > use adtech_nosql
  > db.users.findOne()
  ```
  *Take a screenshot of a user document in mongosh.*
