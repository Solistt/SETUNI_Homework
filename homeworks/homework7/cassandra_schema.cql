CREATE KEYSPACE IF NOT EXISTS amazon_reviews
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE amazon_reviews;

-- Endpoint 1: GET /reviews/product/{product_id}
CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id text,
    review_date date,
    review_id text,
    customer_id text,
    star_rating int,
    verified_purchase boolean,
    review_headline text,
    review_body text,
    marketplace text,
    product_title text,
    PRIMARY KEY (product_id, review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- Endpoint 2: GET /reviews/product/{product_id}/rating/{star_rating}
CREATE TABLE IF NOT EXISTS reviews_by_product_rating (
    product_id text,
    star_rating int,
    review_date date,
    review_id text,
    customer_id text,
    verified_purchase boolean,
    review_headline text,
    review_body text,
    marketplace text,
    product_title text,
    PRIMARY KEY ((product_id, star_rating), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- Endpoint 3: GET /reviews/customer/{customer_id}
CREATE TABLE IF NOT EXISTS reviews_by_customer (
    customer_id text,
    review_date date,
    review_id text,
    product_id text,
    star_rating int,
    verified_purchase boolean,
    review_headline text,
    review_body text,
    marketplace text,
    product_title text,
    PRIMARY KEY (customer_id, review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- Endpoint 4: GET /analytics/top-products?start=YYYY-MM&end=YYYY-MM&n=10
CREATE TABLE IF NOT EXISTS product_reviews_by_month (
    year_month text,
    product_id text,
    review_count int,
    PRIMARY KEY (year_month, product_id)
);

-- Endpoint 5: GET /analytics/top-customers?start=YYYY-MM&end=YYYY-MM&n=10
CREATE TABLE IF NOT EXISTS customer_verified_by_month (
    year_month text,
    customer_id text,
    review_count int,
    PRIMARY KEY (year_month, customer_id)
);

-- Endpoint 6: GET /analytics/top-haters?start=YYYY-MM&end=YYYY-MM&n=10
CREATE TABLE IF NOT EXISTS customer_haters_by_month (
    year_month text,
    customer_id text,
    review_count int,
    PRIMARY KEY (year_month, customer_id)
);

-- Endpoint 7: GET /analytics/top-backers?start=YYYY-MM&end=YYYY-MM&n=10
CREATE TABLE IF NOT EXISTS customer_backers_by_month (
    year_month text,
    customer_id text,
    review_count int,
    PRIMARY KEY (year_month, customer_id)
);
