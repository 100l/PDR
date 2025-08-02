from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from database import Database
from stats import Stats
import logging
import os
import sqlite3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TrafficRulesBot:
    def __init__(self, token, admin_id):
        self.db = Database()
        self.stats = Stats()
        self.token = token
        self.admin_id = admin_id
        self.app = Application.builder().token(token).build()
        self.setup_handlers()

    def setup_handlers(self):
        self.app.add_handler(CommandHandler('start', self.start))
        self.app.add_handler(CommandHandler('search', self.search))
        self.app.add_handler(CommandHandler('sections', self.sections))
        self.app.add_handler(CommandHandler('partners', self.partners))
        self.app.add_handler(CommandHandler('stats', self.stats))
        self.app.add_handler(CommandHandler('update_partners', self.update_partners))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        self.app.add_handler(CallbackQueryHandler(self.button))

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.db.log_action(update.effective_user.id, 'start')
        keyboard = [
            [InlineKeyboardButton("Пошук", callback_data='search')],
            [InlineKeyboardButton("Розділи", callback_data='sections')],
            [InlineKeyboardButton("Партнери", callback_data='partners')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text('Вітаємо у боті ПДР України! Виберіть дію:', reply_markup=reply_markup)

    async def search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.db.log_action(update.effective_user.id, 'search')
        await update.message.reply_text('Введіть номер статті (наприклад, "Стаття 1.2") або ключові слова (наприклад, "перевищення швидкості"):')

    async def sections(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.db.log_action(update.effective_user.id, 'sections')
        conn = sqlite3.connect('data/PDR.db')
        cursor = conn.cursor()
        cursor.execute('SELECT id, name FROM sections')
        sections = cursor.fetchall()
        conn.close()
        
        keyboard = [[InlineKeyboardButton(s[1], callback_data=f'section_{s[0]}')] for s in sections]
        keyboard.append([InlineKeyboardButton("Назад", callback_data='main_menu')])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text('Виберіть розділ:', reply_markup=reply_markup)

    async def partners(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.db.log_action(update.effective_user.id, 'partners')
        content = self.db.get_partners_content()
        keyboard = [[InlineKeyboardButton("Назад", callback_data='main_menu')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(content, reply_markup=reply_markup)

    async def stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.admin_id:
            await update.message.reply_text('Ця команда доступна лише для адміністратора.')
            return
        
        self.stats.generate_plots()
        text = (
            f"Статистика за місяць:\n"
            f"Унікальні користувачі: {self.stats.get_unique_users('month')}\n"
            f"Кількість запитів: {self.stats.get_query_count()}\n"
            f"Середній інтервал між діями: {self.stats.get_avg_action_interval(self.admin_id):.2f} сек\n"
            f"Середня кількість статей за сесію: {self.stats.get_avg_articles_per_session():.2f}\n"
            f"Глибина перегляду: {self.stats.get_view_depth():.2f}\n"
            f"Популярні статті:\n" + '\n'.join([f"{a['number']}: {a['views']} переглядів" for a in self.stats.get_article_views()]) + "\n"
            f"Популярні розділи:\n" + '\n'.join([f"{s['section']}: {s['views']} переглядів" for s in self.stats.get_popular_sections()]) + "\n"
            f"Технічні метрики: {self.stats.get_technical_metrics()}\n"
            f"Метрики взаємодії: {self.stats.get_interaction_metrics()}\n"
            f"Поведінкові сегменти: {self.stats.get_behavioral_segments()}"
        )
        await update.message.reply_text(text)
        
        for plot in ['unique_users.png', 'popular_articles.png']:
            with open(f'data/plots/{plot}', 'rb') as f:
                await update.message.reply_photo(f)

    async def update_partners(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.admin_id:
            await update.message.reply_text('Ця команда доступна лише для адміністратора.')
            return
        if not context.args:
            await update.message.reply_text('Введіть новий вміст для кнопки "Партнери":')
            return
        new_content = ' '.join(context.args)
        self.db.update_partners_content(new_content)
        await update.message.reply_text('Вміст кнопки "Партнери" оновлено!')

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.message.text
        self.db.log_action(update.effective_user.id, 'search', query=query)
        results = self.db.search_articles(query)
        
        if not results:
            await update.message.reply_text('Статтю не знайдено. Спробуйте інші ключові слова.')
            return
        
        article = results[0]
        conn = sqlite3.connect('data/PDR.db')
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM articles WHERE section_id = ? AND number < ?', 
                      (article['section_id'], article['number']))
        prev_id = cursor.fetchone()
        cursor.execute('SELECT id FROM articles WHERE section_id = ? AND number > ?', 
                      (article['section_id'], article['number']))
        next_id = cursor.fetchone()
        conn.close()

        keyboard = []
        if prev_id:
            keyboard.append(InlineKeyboardButton("Назад", callback_data=f'article_{prev_id[0]}'))
        if next_id:
            keyboard.append(InlineKeyboardButton("Вперед", callback_data=f'article_{next_id[0]}'))
        keyboard.append([InlineKeyboardButton("Розділи", callback_data='sections')])
        keyboard.append([InlineKeyboardButton("Партнери", callback_data='partners')])
        reply_markup = InlineKeyboardMarkup([keyboard])

        text = f"{article['section']}\n{article['number']}\n{article['text']}"
        self.db.log_action(update.effective_user.id, 'view_article', article_id=article['id'])
        await update.message.reply_text(text, reply_markup=reply_markup)

    async def button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data

        if data == 'main_menu':
            keyboard = [
                [InlineKeyboardButton("Пошук", callback_data='search')],
                [InlineKeyboardButton("Розділи", callback_data='sections')],
                [InlineKeyboardButton("Партнери", callback_data='partners')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text('Виберіть дію:', reply_markup=reply_markup)
        elif data == 'search':
            self.db.log_action(update.effective_user.id, 'search')
            await query.message.reply_text('Введіть номер статті або ключові слова:')
        elif data == 'sections':
            await self.sections(query, context)
        elif data == 'partners':
            await self.partners(query, context)
        elif data.startswith('section_'):
            section_id = int(data.split('_')[1])
            conn = sqlite3.connect('data/PDR.db')
            cursor = conn.cursor()
            cursor.execute('SELECT id, number, title FROM articles WHERE section_id = ?', (section_id,))
            articles = cursor.fetchall()
            conn.close()
            
            keyboard = [[InlineKeyboardButton(f"{a[1]}: {a[2]}", callback_data=f'article_{a[0]}')] for a in articles]
            keyboard.append([InlineKeyboardButton("Назад", callback_data='main_menu')])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text('Виберіть статтю:', reply_markup=reply_markup)
        elif data.startswith('article_'):
            article_id = int(data.split('_')[1])
            conn = sqlite3.connect('data/PDR.db')
            cursor = conn.cursor()
            cursor.execute('''
                SELECT a.id, a.number, a.title, a.text, s.name, a.section_id
                FROM articles a
                JOIN sections s ON a.section_id = s.id
                WHERE a.id = ?
            ''', (article_id,))
            article = cursor.fetchone()
            cursor.execute('SELECT id FROM articles WHERE section_id = ? AND number < ?', 
                         (article[5], article[1]))
            prev_id = cursor.fetchone()
            cursor.execute('SELECT id FROM articles WHERE section_id = ? AND number > ?', 
                         (article[5], article[1]))
            next_id = cursor.fetchone()
            conn.close()

            keyboard = []
            if prev_id:
                keyboard.append(InlineKeyboardButton("Назад", callback_data=f'article_{prev_id[0]}'))
            if next_id:
                keyboard.append(InlineKeyboardButton("Вперед", callback_data=f'article_{next_id[0]}'))
            keyboard.append([InlineKeyboardButton("Розділи", callback_data='sections')])
            keyboard.append([InlineKeyboardButton("Партнери", callback_data='partners')])
            reply_markup = InlineKeyboardMarkup([keyboard])

            text = f"{article[4]}\n{article[1]}\n{article[3]}"
            self.db.log_action(update.effective_user.id, 'view_article', article_id=article_id)
            await query.message.reply_text(text, reply_markup=reply_markup)

    def run(self):
        self.app.run_polling()

if __name__ == "__main__":
    import os
    TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    ADMIN_ID = int(os.getenv('ADMIN_ID', '0'))
    bot = TrafficRulesBot(TOKEN, ADMIN_ID)
    bot.run()