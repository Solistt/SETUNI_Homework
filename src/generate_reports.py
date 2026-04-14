import mysql.connector
import json
import os

from .config import Config
from .utils import CustomJSONEncoder


class MySQLReportGenerator:
    """
    Generates a suite of campaign and advertiser reports from a MySQL database.
    Database credentials are loaded securely from the root `.env` via `Config`.
    """

    def __init__(self, config: Config):
        self.mysql_cfg = config.get_mysql_config()
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = mysql.connector.connect(**self.mysql_cfg)
        self.cursor = self.conn.cursor(dictionary=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def run_all_reports(self, start_date='2024-10-01 00:00:00', end_date='2024-10-31 23:59:59'):
        """Generate all reports and save them to a JSON file."""
        reports = {}
        print("Generating reports...")

        # 1. Top 5 campaigns by CTR
        print("--> 1. Campaign Performance (CTR)")
        q1 = """
            SELECT c.name AS campaign_name,
                   (COUNT(cl.click_id) / NULLIF(COUNT(i.impression_id),0)) * 100 AS ctr_percentage
            FROM campaigns c
            JOIN impressions i ON c.campaign_id = i.campaign_id
            LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
            WHERE i.timestamp BETWEEN %s AND %s
            GROUP BY c.campaign_id, c.name
            ORDER BY ctr_percentage DESC
            LIMIT 5;
        """
        self.cursor.execute(q1, (start_date, end_date))
        reports['top_campaigns_ctr'] = self.cursor.fetchall()

        # 2. Advertiser spending
        print("--> 2. Advertiser Spending")
        q2 = """
            SELECT a.name AS advertiser_name, SUM(i.cost_paid) AS total_spent
            FROM advertisers a
            JOIN campaigns c ON a.advertiser_id = c.advertiser_id
            JOIN impressions i ON c.campaign_id = i.campaign_id
            WHERE i.timestamp BETWEEN %s AND %s
            GROUP BY a.advertiser_id, a.name
            ORDER BY total_spent DESC;
        """
        self.cursor.execute(q2, (start_date, end_date))
        reports['advertiser_spending'] = self.cursor.fetchall()

        # 3. Cost efficiency: CPC and CPM
        print("--> 3. Cost Efficiency (CPC & CPM)")
        self.cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_campaign_metrics;")
        self.cursor.execute("""
            CREATE TEMPORARY TABLE tmp_campaign_metrics AS
            SELECT c.campaign_id, c.name, SUM(i.cost_paid) AS total_cost,
                   COUNT(i.impression_id) AS total_impressions, COUNT(cl.click_id) AS total_clicks
            FROM campaigns c
            JOIN impressions i ON c.campaign_id = i.campaign_id
            LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
            GROUP BY c.campaign_id, c.name;
        """)
        self.cursor.execute("""
            SELECT name AS campaign_name,
                   total_cost / NULLIF(total_clicks, 0) AS avg_cpc,
                   (total_cost / NULLIF(total_impressions, 0)) * 1000 AS avg_cpm
            FROM tmp_campaign_metrics;
        """)
        reports['cost_efficiency'] = self.cursor.fetchall()

        # 4. Regional revenue
        print("--> 4. Regional Revenue")
        self.cursor.execute("""
            SELECT i.location, SUM(cl.revenue_generated) AS total_revenue
            FROM impressions i JOIN clicks cl ON i.impression_id = cl.impression_id
            GROUP BY i.location ORDER BY total_revenue DESC LIMIT 10;
        """)
        reports['regional_revenue'] = self.cursor.fetchall()

        # 5. Top users by clicks
        print("--> 5. Top Users by Clicks")
        self.cursor.execute("""
            SELECT u.user_id, COUNT(cl.click_id) AS total_clicks
            FROM users u
            JOIN impressions i ON u.user_id = i.user_id
            JOIN clicks cl ON i.impression_id = cl.impression_id
            GROUP BY u.user_id ORDER BY total_clicks DESC LIMIT 10;
        """)
        reports['top_users'] = self.cursor.fetchall()

        # 6. Campaigns with >80% spend
        print("--> 6. Budget Consumption Warnings")
        self.cursor.execute("""
            SELECT name AS campaign_name, budget, remaining_budget,
                   ((budget - remaining_budget) / budget) * 100 AS budget_spent_percentage
            FROM campaigns
            WHERE ((budget - remaining_budget) / budget) > 0.8;
        """)
        reports['budget_warnings'] = self.cursor.fetchall()

        # 7. Device performance (CTR)
        print("--> 7. Device Performance")
        self.cursor.execute("""
            SELECT i.device,
                   (COUNT(cl.click_id) / NULLIF(COUNT(i.impression_id),0)) * 100 AS ctr_percentage
            FROM impressions i
            LEFT JOIN clicks cl ON i.impression_id = cl.impression_id
            GROUP BY i.device ORDER BY ctr_percentage DESC;
        """)
        reports['device_performance'] = self.cursor.fetchall()

        # Write the reports to JSON
        output_dir = 'output/hw2_reports'
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'ad_campaign_performance_report.json')

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(reports, f, indent=4, cls=CustomJSONEncoder)

        print(f"--> Report written to {output_file}")
        return reports


if __name__ == "__main__":
    try:
        config = Config.load_from_env()
        with MySQLReportGenerator(config) as reporter:
            reporter.run_all_reports()
    except Exception as e:
        print(f"An error occurred during report generation: {e}")