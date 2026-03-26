-- Додавання індексів для продуктивності 
ALTER TABLE impressions ADD INDEX idx_timestamp (timestamp);
ALTER TABLE impressions ADD INDEX idx_device (device);
ALTER TABLE impressions ADD INDEX idx_location (location);
ALTER TABLE impressions ADD INDEX idx_user_id (user_id);



--- dataset виділено в один місяць для економії часу і ресурсів 
-- 1. Кампанії з найвищим показником клікабельності CTR (Top 5) за 30 днів
SELECT c.name AS campaign_name,
       (COUNT(cl.click_id) / COUNT(i.impression_id)) * 100 AS ctr_percentage
FROM campaigns c
JOIN impressions i ON c.campaign_id = i.campaign_id
LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
WHERE i.timestamp BETWEEN '2024-10-01 00:00:00' AND '2024-10-31 23:59:59'
GROUP BY c.campaign_id, c.name
ORDER BY ctr_percentage DESC
LIMIT 5;

-- 2. Найбільші рекламодавці за витратами (Advertiser Spending)
SELECT a.name AS advertiser_name,
       SUM(i.cost_paid) AS total_spent
FROM advertisers a
JOIN campaigns c ON a.advertiser_id = c.advertiser_id
JOIN impressions i ON c.campaign_id = i.campaign_id
WHERE i.timestamp BETWEEN '2024-10-01 00:00:00' AND '2024-10-31 23:59:59'
GROUP BY a.advertiser_id, a.name
ORDER BY total_spent DESC;

-- 3. Економічна ефективність (Cost Efficiency: CPC та CPM)
-- Замість повільних CTE використовуємо тимчасову таблицю для максимальної швидкодії 
DROP TEMPORARY TABLE IF EXISTS tmp_campaign_metrics;
CREATE TEMPORARY TABLE tmp_campaign_metrics AS
SELECT c.campaign_id,
       c.name,
       SUM(i.cost_paid) AS total_cost,
       COUNT(i.impression_id) AS total_impressions,
       COUNT(cl.click_id) AS total_clicks
FROM campaigns c
JOIN impressions i ON c.campaign_id = i.campaign_id
LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
GROUP BY c.campaign_id, c.name;

SELECT name AS campaign_name,
       total_cost / NULLIF(total_clicks, 0) AS avg_cpc,
       (total_cost / NULLIF(total_impressions, 0)) * 1000 AS avg_cpm
FROM tmp_campaign_metrics;

-- 4. Регіональний аналіз (Топ локацій за згенерованим доходом)
SELECT i.location,
       SUM(cl.revenue_generated) AS total_revenue
FROM impressions i
JOIN clicks cl ON i.impression_id = cl.impression_id
GROUP BY i.location
ORDER BY total_revenue DESC
LIMIT 10;

-- 5. Залученість користувачів (юзерів за кількістю кліків)
SELECT u.user_id,
       COUNT(cl.click_id) AS total_clicks
FROM users u
JOIN impressions i ON u.user_id = i.user_id
JOIN clicks cl ON i.impression_id = cl.impression_id
GROUP BY u.user_id
ORDER BY total_clicks DESC
LIMIT 10;

-- 6. Споживання бюджету (Кампанії, що витратили понад 80% виділених коштів)
SELECT name AS campaign_name,
       budget,
       remaining_budget,
       ((budget - remaining_budget) / budget) * 100 AS budget_spent_percentage
FROM campaigns
WHERE ((budget - remaining_budget) / budget) > 0.8;

-- 7. Порівняння продуктивності пристроїв (CTR по типах девайсів)
SELECT i.device,
       (COUNT(cl.click_id) / COUNT(i.impression_id)) * 100 AS ctr_percentage
FROM impressions i
LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
GROUP BY i.device
ORDER BY ctr_percentage DESC;