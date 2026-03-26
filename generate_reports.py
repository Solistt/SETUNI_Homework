import pandas as pd
import mysql.connector
import json
import os

# конфіг БД
config = {
    'user': 'root',
    'password': 'root_password',
    'host': '127.0.0.1',
    'database': 'adtech_db'
}

def generate_reports():
    print("--> Nawiązywanie połączenia z bazą danych ")
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor(dictionary=True)

    reports = {}
    
    # Виділення рамок часу
    start_date = '2024-10-01 00:00:00'
    end_date = '2024-10-31 23:59:59'

    try:
        # Кампанії з найвищим показником клікабельності CTR (Top 5)
        print("-->  1. Campaign Performance (CTR) ")
        q1 = """
            SELECT c.name AS campaign_name,
                   (COUNT(cl.click_id) / COUNT(i.impression_id)) * 100 AS ctr_percentage
            FROM campaigns c
            JOIN impressions i ON c.campaign_id = i.campaign_id
            LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
            WHERE i.timestamp BETWEEN %s AND %s
            GROUP BY c.campaign_id, c.name
            ORDER BY ctr_percentage DESC
            LIMIT 5;
        """
        cursor.execute(q1, (start_date, end_date))
        reports['top_campaigns_ctr'] = cursor.fetchall()

        # Найбільші рекламодавці за витратами (Advertiser Spending)
        print("-->: 2. Advertiser Spending ")
        q2 = """
            SELECT a.name AS advertiser_name,
                   SUM(i.cost_paid) AS total_spent
            FROM advertisers a
            JOIN campaigns c ON a.advertiser_id = c.advertiser_id
            JOIN impressions i ON c.campaign_id = i.campaign_id
            WHERE i.timestamp BETWEEN %s AND %s
            GROUP BY a.advertiser_id, a.name
            ORDER BY total_spent DESC;
        """
        cursor.execute(q2, (start_date, end_date))
        reports['advertiser_spending'] = cursor.fetchall()

        # PЕкономічна ефективність (Cost Efficiency: CPC та CPM)
        print("--> : 3. Cost Efficiency (CPC & CPM)")
   
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_campaign_metrics;")
        tmp_metrics = """
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
        """
        cursor.execute(tmp_metrics)
        
        q3 = """
            SELECT name AS campaign_name,
                   total_cost / NULLIF(total_clicks, 0) AS avg_cpc,
                   (total_cost / NULLIF(total_impressions, 0)) * 1000 AS avg_cpm
            FROM tmp_campaign_metrics;
        """
        cursor.execute(q3)
        reports['cost_efficiency'] = cursor.fetchall()

        # Регіональний аналіз (Топ локацій за згенерованим доходом)
        print("--> : 4. Regional Analysis ")
        q4 = """
            SELECT i.location,
                   SUM(cl.revenue_generated) AS total_revenue
            FROM impressions i
            JOIN clicks cl ON i.impression_id = cl.impression_id
            GROUP BY i.location
            ORDER BY total_revenue DESC
            LIMIT 10;
        """
        cursor.execute(q4)
        reports['regional_revenue'] = cursor.fetchall()

        # Залученість користувачів (Топ 10 юзерів за кількістю кліків)
        print("--> Generowanie raportu: 5. User Engagement ")
        q5 = """
            SELECT u.user_id,
                   COUNT(cl.click_id) AS total_clicks
            FROM users u
            JOIN impressions i ON u.user_id = i.user_id
            JOIN clicks cl ON i.impression_id = cl.impression_id
            GROUP BY u.user_id
            ORDER BY total_clicks DESC
            LIMIT 10;
        """
        cursor.execute(q5)
        reports['top_users'] = cursor.fetchall()

        # ті що витратили понад 80% виділених коштів
        print("--> : 6. Budget Consumption ")
        q6 = """
            SELECT name AS campaign_name,
                   budget,
                   remaining_budget,
                   ((budget - remaining_budget) / budget) * 100 AS budget_spent_percentage
            FROM campaigns
            WHERE ((budget - remaining_budget) / budget) > 0.8;
        """
        cursor.execute(q6)
        reports['budget_warnings'] = cursor.fetchall()

        # Порівняння продуктивності пристроїв (CTR по типах девайсів)
        print("--> : 7. Device Performance ")
        q7 = """
            SELECT i.device,
                   (COUNT(cl.click_id) / COUNT(i.impression_id)) * 100 AS ctr_percentage
            FROM impressions i
            LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
            GROUP BY i.device
            ORDER BY ctr_percentage DESC;
        """
        cursor.execute(q7)
        reports['device_performance'] = cursor.fetchall()

        # записуєм в json
        output_file = 'ad_campaign_performance_report.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            
            json.dump(reports, f, indent=4, default=str)
            
        print(f"--> рапорт в {output_file}")

    except Exception as e:
        print(f"error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    generate_reports()