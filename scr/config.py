import os
from dotenv import load_dotenv
from .logger import logger

UPDATE_INTERVAL_MINUTES = 30

SAMPLE_SPREADSHEET_ID = '1Ip007nokkskGbPsu44DF7klmvKs-MPyVZZA2wOWcWWM' #id таблицы на гугл драйв
SQLITE_DB_NAME = 'data.db'



# Получаем путь к текущей директории скрипта
current_dir = os.path.dirname(os.path.abspath(__file__))

# Формируем путь к .env файлу, который находится на уровень выше
dotenv_path = os.path.join(current_dir, '..', '.env')

# Проверяем существование файла
if os.path.exists(dotenv_path):
    # Загружаем переменные из .env файла
    load_dotenv(dotenv_path)
    logger.info(f"Загружены переменные окружения из файла: {dotenv_path}")
else:
    logger.warning(f"Файл .env не найден по пути: {dotenv_path}")





# Яндекс маркет
Tech_PC_Components_YM = os.getenv('Tech_PC_Components_YM')
B_id_Tech_PC_Components_YM = "76443469"

SSmart_shop_YM = os.getenv('SSmart_shop_YM')
B_id_SSmart_shop_YM ="121883700"

ByMarket_YM = os.getenv('ByMarket_YM')
B_id_ByMarket_YM = "95137059"
