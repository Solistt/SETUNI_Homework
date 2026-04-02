
DROP TABLE IF EXISTS clicks;
DROP TABLE IF EXISTS impressions;
DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS advertisers;

-- 
CREATE TABLE advertisers (
    advertiser_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL UNIQUE
) ENGINE=InnoDB;

-- sers.csv
CREATE TABLE users (
    user_id INT PRIMARY KEY,
    age INT,
    gender VARCHAR(20),
    location VARCHAR(100),
    interests TEXT,
    signup_date DATE
) ENGINE=InnoDB;

-- campaigns.csv
CREATE TABLE campaigns (
    campaign_id INT PRIMARY KEY,
    advertiser_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    start_date DATE,
    end_date DATE,
    targeting_criteria TEXT,
    ad_slot_size VARCHAR(50),
    budget DECIMAL(15, 2),
    remaining_budget DECIMAL(15, 2),
    INDEX idx_camp_name (name), -- Added for high-performance ETL joins
    CONSTRAINT fk_adv FOREIGN KEY (advertiser_id) REFERENCES advertisers(advertiser_id)
) ENGINE=InnoDB;

CREATE TABLE impressions (
    impression_id BINARY(16) PRIMARY KEY,
    campaign_id INT NOT NULL,
    user_id INT NOT NULL,
    device VARCHAR(50),
    location VARCHAR(100),
    timestamp DATETIME,
    bid_amount DECIMAL(10, 4),
    cost_paid DECIMAL(10, 4),
    INDEX idx_camp (campaign_id),
    CONSTRAINT fk_camp FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(user_id)
) ENGINE=InnoDB;


CREATE TABLE clicks (
    click_id INT PRIMARY KEY AUTO_INCREMENT,
    impression_id BINARY(16) NOT NULL UNIQUE,
    click_timestamp DATETIME,
    revenue_generated DECIMAL(10, 4),
    CONSTRAINT fk_imp FOREIGN KEY (impression_id) REFERENCES impressions(impression_id)
) ENGINE=InnoDB;