import sqlite3
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Database:
    def __init__(self, db_path='data/PDR.db'):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                timestamp DATETIME NOT NULL,
                action TEXT NOT NULL,
                article_id INTEGER,
                query TEXT,
                FOREIGN KEY (article_id) REFERENCES articles(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                start_time DATETIME NOT NULL,
                end_time DATETIME,
                article_count INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS partners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL
            )
        ''')
        cursor.execute('INSERT OR IGNORE INTO partners (id, content) VALUES (1, ?)', 
                      ('Приєднуйтесь до нашого каналу: t.me/example',))
        conn.commit()
        conn.close()

    def search_articles(self, query):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT a.id, a.number, a.title, a.text, s.name
                FROM articles a
                JOIN sections s ON a.section_id = s.id
                WHERE a.number = ?
            ''', (query,))
            result = cursor.fetchone()
            if result:
                return [{'id': result[0], 'number': result[1], 'title': result[2], 
                         'text': result[3], 'section': result[4]}]
            
            cursor.execute('''
                SELECT a.id, a.number, a.title, a.text, s.name
                FROM articles_fts fts
                JOIN articles a ON fts.rowid = a.id
                JOIN sections s ON a.section_id = s.id
                WHERE articles_fts MATCH ?
                ORDER BY rank
            ''', (query,))
            results = cursor.fetchall()
            return [{'id': r[0], 'number': r[1], 'title': r[2], 'text': r[3], 'section': r[4]} 
                    for r in results]
        finally:
            conn.close()

    def log_action(self, user_id, action, article_id=None, query=None):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            timestamp = datetime.now()
            cursor.execute('''
                INSERT INTO user_logs (user_id, timestamp, action, article_id, query)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, timestamp, action, article_id, query))
            
            cursor.execute('''
                SELECT id, start_time, article_count
                FROM sessions
                WHERE user_id = ? AND end_time IS NULL
                ORDER BY start_time DESC
                LIMIT 1
            ''', (user_id,))
            session = cursor.fetchone()
            
            if session and (timestamp - datetime.fromisoformat(session[1])).total_seconds() <= 30 * 60:
                cursor.execute('''
                    UPDATE sessions
                    SET article_count = article_count + 1
                    WHERE id = ?
                ''', (session[0],))
            else:
                cursor.execute('''
                    INSERT INTO sessions (user_id, start_time, article_count)
                    VALUES (?, ?, 1)
                ''', (user_id, timestamp))
            
            conn.commit()
        finally:
            conn.close()

    def get_partners_content(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT content FROM partners WHERE id = 1')
            return cursor.fetchone()[0]
        finally:
            conn.close()

    def update_partners_content(self, content):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('UPDATE partners SET content = ? WHERE id = 1', (content,))
            conn.commit()
        finally:
            conn.close()