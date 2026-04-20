// MongoDB initialisation script – creates indexes for Homework 6 collections.
// Runs automatically on first container start-up.

db = db.getSiblingDB("amazon_reviews");

// product_reviews_summary – fast look-up by product_id
db.createCollection("product_reviews_summary");
db.product_reviews_summary.createIndex({ product_id: 1 }, { unique: true });

// customer_review_counts – fast look-up by customer_id
db.createCollection("customer_review_counts");
db.customer_review_counts.createIndex({ customer_id: 1 }, { unique: true });

// monthly_product_reviews – trend queries (compound index)
db.createCollection("monthly_product_reviews");
db.monthly_product_reviews.createIndex({ product_id: 1, year: 1, month: 1 }, { unique: true });

print("Homework 6 indexes created.");
