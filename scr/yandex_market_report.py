import aiohttp
import asyncio
import json
import zipfile
import io
import pandas as pd
from datetime import datetime
from scr.logger import logger


async def generate_price_report(session, api_key, business_id, marketname, username):
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

    logger.info(
        f"Отправка запроса на генерацию отчета. URL: {url}",
        extra={"marketname": marketname, "username": username}
    )
    logger.debug(
        f"Заголовки запроса: {headers}",
        extra={"marketname": marketname, "username": username}
    )
    logger.debug(
        f"Параметры запроса: {params}",
        extra={"marketname": marketname, "username": username}
    )
    logger.debug(
        f"Тело запроса: {json.dumps(data, ensure_ascii=False, indent=2)}",
        extra={"marketname": marketname, "username": username}
    )

    async with session.post(url, headers=headers, params=params, json=data) as response:
        logger.info(
            f"Получен ответ. Код статуса: {response.status}",
            extra={"marketname": marketname, "username": username}
        )
        response_text = await response.text()
        logger.debug(
            f"Тело ответа: {response_text}",
            extra={"marketname": marketname, "username": username}
        )

        if response.status == 200:
            return json.loads(response_text)
        else:
            logger.error(
                f"Ошибка при генерации отчета. Код статуса: {response.status}",
                extra={"marketname": marketname, "username": username}
            )
            return None


async def check_report_status(session, api_key, report_id, marketname, username):
    url = f"https://api.partner.market.yandex.ru/reports/info/{report_id}"
    headers = {
        "Api-Key": api_key
    }

    logger.info(
        f"Проверка статуса отчета. ID отчета: {report_id}",
        extra={"marketname": marketname, "username": username}
    )
    logger.debug(
        f"URL запроса: {url}",
        extra={"marketname": marketname, "username": username}
    )
    logger.debug(
        f"Заголовки запроса: {headers}",
        extra={"marketname": marketname, "username": username}
    )

    async with session.get(url, headers=headers) as response:
        logger.info(
            f"Получен ответ. Код статуса: {response.status}",
            extra={"marketname": marketname, "username": username}
        )
        response_text = await response.text()
        logger.debug(
            f"Тело ответа: {response_text}",
            extra={"marketname": marketname, "username": username}
        )

        if response.status == 200:
            return json.loads(response_text)
        else:
            logger.error(
                f"Ошибка при проверке статуса отчета. Код статуса: {response.status}",
                extra={"marketname": marketname, "username": username}
            )
            return None


async def download_report(session, api_key, file_url, marketname, username):
    headers = {
        "Authorization": f"OAuth {api_key}"
    }

    logger.info(
        f"Начало загрузки отчета. URL файла: {file_url}",
        extra={"marketname": marketname, "username": username}
    )
    logger.debug(
        f"Заголовки запроса: {headers}",
        extra={"marketname": marketname, "username": username}
    )

    async with session.get(file_url, headers=headers) as response:
        logger.info(
            f"Получен ответ. Код статуса: {response.status}",
            extra={"marketname": marketname, "username": username}
        )

        if response.status == 200:
            content = await response.read()
            logger.info(
                f"Отчет успешно загружен. Размер: {len(content)} байт",
                extra={"marketname": marketname, "username": username}
            )
            return content
        else:
            logger.error(
                f"Ошибка при загрузке отчета. Код статуса: {response.status}",
                extra={"marketname": marketname, "username": username}
            )
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


async def get_yandex_market_report(api_key, business_id, marketname="YandexMarket", username="DefaultUser"):
    logger.info(
        "Начало процесса получения отчета с Яндекс.Маркета",
        extra={"marketname": marketname, "username": username}
    )
    async with aiohttp.ClientSession() as session:
        # Генерация отчета
        logger.info(
            "Запуск процесса генерации отчета",
            extra={"marketname": marketname, "username": username}
        )
        report_info = await generate_price_report(session, api_key, business_id, marketname, username)
        if not report_info:
            logger.error(
                "Не удалось сгенерировать отчет",
                extra={"marketname": marketname, "username": username}
            )
            return None

        report_id = report_info['result']['reportId']
        estimated_time = report_info['result']['estimatedGenerationTime'] / 1000

        logger.info(
            f"Генерация отчета начата. ID отчета: {report_id}",
            extra={"marketname": marketname, "username": username}
        )
        logger.info(
            f"Ожидаемое время генерации: {estimated_time} секунд",
            extra={"marketname": marketname, "username": username}
        )

        # Ожидание генерации отчета
        while True:
            await asyncio.sleep(10)
            logger.info(
                "Проверка статуса отчета...",
                extra={"marketname": marketname, "username": username}
            )
            status_info = await check_report_status(session, api_key, report_id, marketname, username)
            if not status_info:
                logger.error(
                    "Не удалось получить статус отчета",
                    extra={"marketname": marketname, "username": username}
                )
                return None

            status = status_info['result']['status']
            logger.info(
                f"Текущий статус: {status}",
                extra={"marketname": marketname, "username": username}
            )

            if status == 'DONE':
                logger.info(
                    "Отчет готов",
                    extra={"marketname": marketname, "username": username}
                )
                file_url = status_info['result']['file']
                logger.info(
                    f"URL для скачивания: {file_url}",
                    extra={"marketname": marketname, "username": username}
                )
                break
            elif status in ['FAILED', 'NO_DATA']:
                logger.error(
                    f"Произошла ошибка при генерации отчета: {status}",
                    extra={"marketname": marketname, "username": username}
                )
                if 'subStatus' in status_info['result']:
                    logger.error(
                        f"Дополнительный статус: {status_info['result']['subStatus']}",
                        extra={"marketname": marketname, "username": username}
                    )
                return None
            else:
                logger.info(
                    "Отчет все еще генерируется...",
                    extra={"marketname": marketname, "username": username}
                )

        # Скачивание отчета
        logger.info(
            "Начало скачивания отчета",
            extra={"marketname": marketname, "username": username}
        )
        report_content = await download_report(session, api_key, file_url, marketname, username)
        if report_content:
            # Обработка CSV-данных из ZIP-архива
            logger.info(
                "Обработка загруженного отчета",
                extra={"marketname": marketname, "username": username}
            )
            return process_csv_from_zip(report_content)
        else:
            logger.error(
                "Не удалось скачать отчет",
                extra={"marketname": marketname, "username": username}
            )
            return None


# Пример использования функции
async def main():
    API_KEY = "ACMA:D4a5OExH6Hvtcx8BxgTqv2gfIpc2E7KmTPlekqDE:43a81531"
    BUSINESS_ID = 76443469

    df = await get_yandex_market_report(
        API_KEY,
        BUSINESS_ID,
        marketname="YandexMarket",
        username="TestUser"
    )

    if df is not None:
        print(df.head())
        print(df.info())
    else:
        print("Не удалось получить отчет")


if __name__ == "__main__":
    asyncio.run(main())









