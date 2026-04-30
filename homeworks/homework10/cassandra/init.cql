CREATE KEYSPACE IF NOT EXISTS hw10
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS hw10.page_creations (
    domain text,
    created_at timestamp,
    page_title text,
    user_id text,
    PRIMARY KEY ((domain), created_at, page_title, user_id)
) WITH CLUSTERING ORDER BY (created_at DESC, page_title ASC, user_id ASC);
