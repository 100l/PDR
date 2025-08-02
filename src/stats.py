import sqlite3
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Stats:
    def __init__(self, db_path='data/PDR.db'):
        self.db_path = db_path
        self.output_dir = 'data/plots'
        os.makedirs(self.output_dir, exist_ok=True)

    def get_unique_users(self, period):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            if period == 'day':
                time_filter = "DATE(timestamp) = DATE('now')"
            elif period == 'week':
                time_filter = "DATE(timestamp) >= DATE('now', '-7 days')"
            elif period == 'month':
                time_filter = "DATE(timestamp) >= DATE('now', '-30 days')"
            else:
                time_filter = "DATE(timestamp) >= DATE('now', '-365 days')"
            
            cursor.execute(f'''
                SELECT COUNT(DISTINCT user_id)
                FROM user_logs
                WHERE {time_filter}
            ''')
            return cursor.fetchone()[0]
        finally:
            conn.close()

    def get_query_count(self, user_id=None):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            if user_id:
                cursor.execute('''
                    SELECT COUNT(*)
                    FROM user_logs
                    WHERE action = 'search' AND user_id = ?
                ''', (user_id,))
            else:
                cursor.execute('''
                    SELECT COUNT(*)
                    FROM user_logs
                    WHERE action = 'search'
                ''')
            return cursor.fetchone()[0]
        finally:
            conn.close()

    def get_article_views(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT a.number, COUNT(*) as views
                FROM user_logs l
                JOIN articles a ON l.article_id = a.id
                WHERE l.action = 'view_article'
                GROUP BY a.id
                ORDER BY views DESC
                LIMIT 5
            ''')
            return [{'number': r[0], 'views': r[1]} for r in cursor.fetchall()]
        finally:
            conn.close()

    def get_avg_action_interval(self, user_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT timestamp
                FROM user_logs
                WHERE user_id = ?
                ORDER BY timestamp
            ''', (user_id,))
            timestamps = [datetime.fromisoformat(r[0]) for r in cursor.fetchall()]
            if len(timestamps) < 2:
                return 0
            intervals = [(timestamps[i+1] - timestamps[i]).total_seconds() for i in range(len(timestamps)-1)]
            return sum(intervals) / len(intervals)
        finally:
            conn.close()

    def get_avg_articles_per_session(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT AVG(article_count)
                FROM sessions
                WHERE end_time IS NOT NULL
            ''')
            result = cursor.fetchone()[0]
            return result or 0
        finally:
            conn.close()

    def get_popular_sections(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT s.name, COUNT(*) as views
                FROM user_logs l
                JOIN articles a ON l.article_id = a.id
                JOIN sections s ON a.section_id = s.id
                WHERE l.action = 'view_article'
                GROUP BY s.id
                ORDER BY views DESC
                LIMIT 5
            ''')
            return [{'section': r[0], 'views': r[1]} for r in cursor.fetchall()]
        finally:
            conn.close()

    def get_view_depth(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT AVG(article_count)
                FROM sessions
                WHERE article_count > 0
            ''')
            result = cursor.fetchone()[0]
            return result or 0
        finally:
            conn.close()

    def get_technical_metrics(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT COUNT(*)
                FROM user_logs
                WHERE action LIKE 'callback%'
            ''')
            callbacks = cursor.fetchone()[0]
            cursor.execute('''
                SELECT COUNT(*)
                FROM user_logs
                WHERE action = 'error'
            ''')
            errors = cursor.fetchone()[0]
            return {'callbacks': callbacks, 'errors': errors}
        finally:
            conn.close()

    def get_interaction_metrics(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT COUNT(DISTINCT user_id)
                FROM user_logs
                GROUP BY user_id
                HAVING COUNT(*) = 1
            ''')
            drop_off = cursor.fetchone()[0] if cursor.fetchone() else 0

            cursor.execute('''
                SELECT AVG((julianday(end_time) - julianday(start_time)) * 86400)
                FROM sessions
                WHERE end_time IS NOT NULL
            ''')
            avg_session_duration = cursor.fetchone()[0] or 0

            return {'drop_off': drop_off, 'avg_session_duration': avg_session_duration}
        finally:
            conn.close()

    def get_behavioral_segments(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT user_id, COUNT(*) as actions
                FROM user_logs
                GROUP BY user_id
            ''')
            segments = {'one_time': 0, 'regular': 0, 'engaged': 0}
            for user_id, actions in cursor.fetchall():
                if actions == 1:
                    segments['one_time'] += 1
                elif actions <= 10:
                    segments['regular'] += 1
                else:
                    segments['engaged'] += 1
            return segments
        finally:
            conn.close()

    def generate_plots(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT DATE(timestamp) as day, COUNT(DISTINCT user_id) as users
                FROM user_logs
                WHERE DATE(timestamp) >= DATE('now', '-30 days')
                GROUP BY day
                ORDER BY day
            ''')
            data = cursor.fetchall()
            days = [r[0] for r in data]
            users = [r[1] for r in data]
            plt.figure(figsize=(10, 5))
            plt.plot(days, users, marker='o')
            plt.title('Унікальні користувачі за днями')
            plt.xlabel('Дата')
            plt.ylabel('Кількість користувачів')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(f'{self.output_dir}/unique_users.png')
            plt.close()

            cursor.execute('''
                SELECT a.number, COUNT(*) as views
                FROM user_logs l
                JOIN articles a ON l.article_id = a.id
                WHERE l.action = 'view_article'
                GROUP BY a.id
                ORDER BY views DESC
                LIMIT 5
            ''')
            data = cursor.fetchall()
            articles = [r[0] for r in data]
            views = [r[1] for r in data]
            plt.figure(figsize=(10, 5))
            plt.bar(articles, views)
            plt.title('Популярні статті')
            plt.xlabel('Стаття')
            plt.ylabel('Перегляди')
            plt.tight_layout()
            plt.savefig(f'{self.output_dir}/popular_articles.png')
            plt.close()
        finally:
            conn.close()