import requests
from bs4 import BeautifulSoup
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def init_db():
    conn = sqlite3.connect('data/PDR.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section_id INTEGER,
            number TEXT NOT NULL,
            title TEXT NOT NULL,
            text TEXT NOT NULL,
            FOREIGN KEY (section_id) REFERENCES sections(id)
        )
    ''')
    cursor.execute('''
        CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
            number, title, text, content='articles', content_rowid='id'
        )
    ''')
    conn.commit()
    return conn, cursor

def parse_traffic_rules(url):
    try:
        conn, cursor = init_db()
        
        cursor.execute('DELETE FROM articles_fts')
        cursor.execute('DELETE FROM articles')
        cursor.execute('DELETE FROM sections')
        conn.commit()
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        content = soup.find('div', {'class': 'doc_inner'})
        if not content:
            raise ValueError("Не вдалося знайти основний контейнер із ПДР")

        current_section = None
        current_section_id = None
        articles = []

        for element in content.find_all(['h2', 'h3', 'p']):
            if element.name == 'h2':
                current_section = element.get_text(strip=True)
                cursor.execute('INSERT OR REPLACE INTO sections (name) VALUES (?)', (current_section,))
                current_section_id = cursor.lastrowid
            elif element.name == 'h3' and current_section_id:
                article_number = element.get_text(strip=True)
                article = {
                    'section_id': current_section_id,
                    'number': article_number,
                    'title': article_number,
                    'text': ''
                }
                articles.append(article)
            elif element.name == 'p' and articles and articles[-1]:
                articles[-1]['text'] += element.get_text(strip=True) + '\n'

        for article in articles:
            cursor.execute('''
                INSERT INTO articles (section_id, number, title, text)
                VALUES (?, ?, ?, ?)
            ''', (article['section_id'], article['number'], article['title'], article['text'].strip()))
            article_id = cursor.lastrowid
            cursor.execute('''
                INSERT INTO articles_fts (rowid, number, title, text)
                VALUES (?, ?, ?, ?)
            ''', (article_id, article['number'], article['title'], article['text'].strip()))
        
        conn.commit()
        logging.info(f"Успішно спарсено та збережено {len(articles)} статей")
        
    except Exception as e:
        logging.error(f"Помилка парсингу: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    url = "https://zakon.rada.gov.ua/laws/show/1306-2001-%D0%BF#Text"
    parse_traffic_rules(url)