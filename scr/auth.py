from pathlib import Path
from google.oauth2 import service_account
from typing import Optional
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger


class GoogleServiceAuthManager:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        self.config_dir = self.base_dir / 'scr'/"access"
        self.service_account_path = self.config_dir / "service-account.json"

        # Создаём директорию если её нет
        self.config_dir.mkdir(parents=True, exist_ok=True)

    async def ensure_credentials_exist(self) -> None:
        """Проверяет наличие файла service-account.json"""
        if not self.service_account_path.exists():
            logger.error(f"Файл service-account.json не найден: {self.service_account_path}")
            raise FileNotFoundError(
                f"Файл service-account.json должен быть размещен в: {self.service_account_path}\n"
                "Получите его в Google Cloud Console -> IAM & Admin -> Service Accounts"
            )

    async def get_credentials(self):
        """Получает учетные данные из файла service-account.json"""
        logger.info("Запуск процесса получения учетных данных сервисного аккаунта")

        await self.ensure_credentials_exist()

        try:
            credentials = service_account.Credentials.from_service_account_file(
                str(self.service_account_path),
                scopes=self.SCOPES
            )
            logger.info("Учетные данные сервисного аккаунта успешно загружены")
            return credentials
        except Exception as e:
            logger.error(f"Ошибка при загрузке учетных данных сервисного аккаунта: {e}")
            raise

        # Пример использования


async def get_credentials():
    auth_manager = GoogleServiceAuthManager()
    return await auth_manager.get_credentials()

