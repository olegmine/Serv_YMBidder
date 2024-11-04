import aiohttp
import asyncio
import json
import zipfile
import io
import pandas as pd
from datetime import datetime
from scr.logger import logger


async def generate_price_report(session, api_key, business_id):
    url = "https://api.partner.market.yandex.ru/reports/prices/generate"
    headers = {
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }
    params = {
        "format": "CSV"
    }
    current_date = datetime.now().strftime("%d-%m-%Y")
    data = {
        "businessId": business_id,
        "categoryIds": [],
        "creationDateFrom": "01-01-2023",
        "creationDateTo": current_date
    }

    logger.info(f"Отправка запроса на генерацию отчета. URL: {url}")
    logger.debug(f"Заголовки запроса: {headers}")
    logger.debug(f"Параметры запроса: {params}")
    logger.debug(f"Тело запроса: {json.dumps(data, ensure_ascii=False, indent=2)}")

    async with session.post(url, headers=headers, params=params, json=data) as response:
        logger.info(f"Получен ответ. Код статуса: {response.status}")
        response_text = await response.text()
        logger.debug(f"Тело ответа: {response_text}")

        if response.status == 200:
            return json.loads(response_text)
        else:
            logger.error(f"Ошибка при генерации отчета. Код статуса: {response.status}")
            return None


async def check_report_status(session, api_key, report_id):
    url = f"https://api.partner.market.yandex.ru/reports/info/{report_id}"
    headers = {
        "Api-Key": api_key
    }

    logger.info(f"Проверка статуса отчета. ID отчета: {report_id}")
    logger.debug(f"URL запроса: {url}")
    logger.debug(f"Заголовки запроса: {headers}")

    async with session.get(url, headers=headers) as response:
        logger.info(f"Получен ответ. Код статуса: {response.status}")
        response_text = await response.text()
        logger.debug(f"Тело ответа: {response_text}")

        if response.status == 200:
            return json.loads(response_text)
        else:
            logger.error(f"Ошибка при проверке статуса отчета. Код статуса: {response.status}")
            return None


async def download_report(session, api_key, file_url):
    headers = {
        "Authorization": f"OAuth {api_key}"
    }

    logger.info(f"Начало загрузки отчета. URL файла: {file_url}")
    logger.debug(f"Заголовки запроса: {headers}")

    async with session.get(file_url, headers=headers) as response:
        logger.info(f"Получен ответ. Код статуса: {response.status}")

        if response.status == 200:
            content = await response.read()
            logger.info(f"Отчет успешно загружен. Размер: {len(content)} байт")
            return content
        else:
            logger.error(f"Ошибка при загрузке отчета. Код статуса: {response.status}")
            return None


def process_csv_from_zip(zip_content):
    logger.info("Начало обработки ZIP-архива с CSV-данными")
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
        file_list = zip_file.namelist()
        logger.info(f"Файлы в архиве: {', '.join(file_list)}")

        for filename in file_list:
            logger.info(f"Обработка файла: {filename}")
            with zip_file.open(filename) as csv_file:
                df = pd.read_csv(csv_file, encoding='utf-8')
                logger.info(f"CSV успешно прочитан. Размер DataFrame: {df.shape}")
                return df


async def get_yandex_market_report(api_key, business_id):
    logger.info("Начало процесса получения отчета с Яндекс.Маркета")
    async with aiohttp.ClientSession() as session:
        # Генерация отчета
        logger.info("Запуск процесса генерации отчета")
        report_info = await generate_price_report(session, api_key, business_id)
        if not report_info:
            logger.error("Не удалось сгенерировать отчет")
            return None

        report_id = report_info['result']['reportId']
        estimated_time = report_info['result']['estimatedGenerationTime'] / 1000

        logger.warning(f"Генерация отчета начата. ID отчета: {report_id}")
        logger.info(f"Ожидаемое время генерации: {estimated_time} секунд")

        # Ожидание генерации отчета
        while True:
            await asyncio.sleep(10)
            logger.info("Проверка статуса отчета...")
            status_info = await check_report_status(session, api_key, report_id)
            if not status_info:
                logger.error("Не удалось получить статус отчета")
                return None

            status = status_info['result']['status']
            logger.info(f"Текущий статус: {status}")

            if status == 'DONE':
                logger.info("Отчет готов")
                file_url = status_info['result']['file']
                logger.info(f"URL для скачивания: {file_url}")
                break
            elif status in ['FAILED', 'NO_DATA']:
                logger.error(f"Произошла ошибка при генерации отчета: {status}")
                if 'subStatus' in status_info['result']:
                    logger.error(f"Дополнительный статус: {status_info['result']['subStatus']}")
                return None
            else:
                logger.info("Отчет все еще генерируется...")

        # Скачивание отчета
        logger.warning("Начало скачивания отчета")
        report_content = await download_report(session, api_key, file_url)
        if report_content:
            # Обработка CSV-данных из ZIP-архива
            logger.info("Обработка загруженного отчета")
            return process_csv_from_zip(report_content)
        else:
            logger.error("Не удалось скачать отчет")
            return None


# Пример использования функции
if __name__ == "__main__":
    # API_KEY = "ACMA:D4a5OExH6Hvtcx8BxgTqv2gfIpc2E7KmTPlekqDE:43a81531"
    # BUSINESS_ID = 76443469


    async def main():
        logger.info("Запуск основной функции")




    asyncio.run(main())












