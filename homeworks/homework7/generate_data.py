#!/usr/bin/env python3
"""
Generate a synthetic Amazon Reviews TSV dataset for Homework 7 testing.
Produces data that exercises ALL 7 API endpoints meaningfully.

Usage:
    python3 generate_data.py                      # writes ./data/amazon_reviews.tsv
    python3 generate_data.py /custom/path.tsv
"""

import csv
import os
import random
import string
import sys
from datetime import date, timedelta

SEED = 42
random.seed(SEED)

# ── Volume controls ────────────────────────────────────────────────────────────
NUM_PRODUCTS = 80        # distinct products
NUM_CUSTOMERS = 120      # distinct customers
NUM_REVIEWS = 6_000      # total review rows
DATE_START = date(2013, 1, 1)
DATE_END   = date(2015, 12, 31)

CATEGORIES = [
    "Books", "Electronics", "Toys", "Sports", "Beauty",
    "Kitchen", "Clothing", "Music", "Video_Games", "Tools",
]
MARKETPLACES = ["US", "UK", "DE", "JP", "FR"]

HEADLINES = [
    "Great product!", "Not what I expected", "Absolutely love it",
    "Would not recommend", "Five stars all day", "Decent but overpriced",
    "Perfect gift", "Broke after one week", "Best purchase ever",
    "Meh, it's okay", "Exceeded my expectations", "Total disappointment",
    "Very happy with this", "Does the job", "Amazing quality",
]
BODIES = [
    "This product is exactly what I needed. Very happy with the purchase.",
    "I was disappointed. The quality is not as described in the listing.",
    "Bought this for my kid and they love it. Would definitely buy again.",
    "Stopped working after a few uses. Not worth the money at all.",
    "Incredible value for the price. Highly recommend to everyone.",
    "Instructions were unclear but the product itself works fine.",
    "Fast shipping and item was exactly as described. Great seller.",
    "Not sure I understand all the hype. Just an average product.",
    "Perfect for what I needed. Fits perfectly and looks great.",
    "The color is slightly different from the pictures but still nice.",
]


def _rand_id(prefix: str, length: int) -> str:
    return prefix + "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def _rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


# Pre-generate product and customer pools
products = [
    {
        "product_id": _rand_id("B0", 8),
        "product_parent": str(random.randint(100_000_000, 999_999_999)),
        "product_title": f"Product {i} - {random.choice(CATEGORIES)}",
        "product_category": random.choice(CATEGORIES),
    }
    for i in range(NUM_PRODUCTS)
]

customers = [_rand_id("A", 13) for _ in range(NUM_CUSTOMERS)]

# ── Write TSV ──────────────────────────────────────────────────────────────────
COLUMNS = [
    "marketplace", "customer_id", "review_id", "product_id", "product_parent",
    "product_title", "product_category", "star_rating", "helpful_votes",
    "total_votes", "vine", "verified_purchase", "review_headline",
    "review_body", "review_date",
]

dest = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(__file__), "data", "amazon_reviews.tsv"
)
os.makedirs(os.path.dirname(os.path.abspath(dest)), exist_ok=True)

review_ids_seen: set = set()

def unique_review_id() -> str:
    while True:
        rid = _rand_id("R", 14)
        if rid not in review_ids_seen:
            review_ids_seen.add(rid)
            return rid


# Make some customers prolific so top-N queries return interesting results
# 10 "power reviewers" get 10× the normal number of reviews
power_customers = random.sample(customers, 10)
# 15 "popular products" get 8× the normal number of reviews
popular_products = random.sample(products, 15)

weighted_customers = customers + power_customers * 9
weighted_products  = products  + popular_products * 7

with open(dest, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(COLUMNS)

    for _ in range(NUM_REVIEWS):
        product  = random.choice(weighted_products)
        customer = random.choice(weighted_customers)
        star     = random.choices([1, 2, 3, 4, 5], weights=[8, 7, 10, 25, 50])[0]
        rev_date = _rand_date(DATE_START, DATE_END)
        helpful  = random.randint(0, 50)
        total    = helpful + random.randint(0, 20)
        verified = random.choices(["Y", "N"], weights=[75, 25])[0]
        vine     = random.choices(["Y", "N"], weights=[5, 95])[0]

        writer.writerow([
            random.choice(MARKETPLACES),
            customer,
            unique_review_id(),
            product["product_id"],
            product["product_parent"],
            product["product_title"],
            product["product_category"],
            star,
            helpful,
            total,
            vine,
            verified,
            random.choice(HEADLINES),
            random.choice(BODIES),
            rev_date.strftime("%Y-%m-%d"),
        ])

print(f"Generated {NUM_REVIEWS} reviews → {dest}")
print(f"  Products : {NUM_PRODUCTS} ({len(popular_products)} popular)")
print(f"  Customers: {NUM_CUSTOMERS} ({len(power_customers)} power-reviewers)")
print(f"  Date range: {DATE_START} – {DATE_END}")
print()
print("Sample API queries to try:")
print(f"  GET /reviews/product/{ popular_products[0]['product_id'] }")
print(f"  GET /reviews/product/{ popular_products[0]['product_id'] }/rating/5")
print(f"  GET /reviews/customer/{ power_customers[0] }")
print(f"  GET /analytics/top-products?start=2013-01&end=2015-12&n=5")
print(f"  GET /analytics/top-customers?start=2013-01&end=2015-12&n=5")
print(f"  GET /analytics/top-haters?start=2013-01&end=2015-12&n=5")
print(f"  GET /analytics/top-backers?start=2013-01&end=2015-12&n=5")
