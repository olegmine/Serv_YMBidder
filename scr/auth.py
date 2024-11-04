import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from .logger import logger

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


async def get_credentials():
    logger.info("Запуск функции get_credentials")

    # Получаем абсолютный путь к директории, где находится этот скрипт
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Определяем пути к файлам относительно директории скрипта
    token_path = os.path.join(script_dir, "acsess/token.json")
    credentials_path = os.path.join(script_dir, "acsess/credentials.json")

    creds = None
    if os.path.exists(token_path):
        logger.debug(f"Найден существующий файл token.json: {token_path}")
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        logger.info("Загружены учетные данные из token.json")

    if not creds or not creds.valid:
        logger.info("Учетные данные отсутствуют или недействительны")
        if creds and creds.expired and creds.refresh_token:
            logger.info("Обновление просроченных учетных данных")
            try:
                creds.refresh(Request())
                logger.info("Учетные данные успешно обновлены")
            except Exception as e:
                logger.error(f"Не удалось обновить учетные данные: {str(e)}")
        else:
            logger.info("Инициация нового процесса аутентификации")
            try:
                if not os.path.exists(credentials_path):
                    logger.error(f"Файл credentials.json не найден по пути: {credentials_path}")
                    raise FileNotFoundError(f"Файл credentials.json не найден по пути: {credentials_path}")

                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
                creds = flow.run_local_server(port=0)
                logger.info("Получены новые учетные данные через локальный сервер")
            except Exception as e:
                logger.error(f"Не удалось получить новые учетные данные: {str(e)}")
                raise

        logger.debug(f"Сохранение новых учетных данных в {token_path}")
        try:
            with open(token_path, "w") as token:
                token.write(creds.to_json())
            logger.info("Новые учетные данные сохранены в token.json")
        except Exception as e:
            logger.error(f"Не удалось сохранить учетные данные в token.json: {str(e)}")

    logger.info("Учетные данные успешно получены")
    return creds

