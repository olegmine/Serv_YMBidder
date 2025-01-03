import asyncio
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from .auth import get_credentials
from typing import Optional, List, Union
import numpy as np
import re
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scr.logger import logger


def safe_number_convert(value: Union[str, float, int]) -> Union[float, str]:
    """
    Безопасное преобразование строки в число.

    Args:
        value: Значение для преобразования

    Returns:
        Union[float, str]: Преобразованное число или исходная строка
    """
    if pd.isna(value) or value == '':
        return ''

        # Если уже число, преобразуем в float
    if isinstance(value, (int, float)):
        return float(value)

    try:
        # Преобразуем в строку и очищаем от пробелов
        str_value = str(value).strip()

        # Заменяем запятую на точку
        str_value = str_value.replace(',', '.')

        # Преобразуем в float
        return float(str_value)

    except (ValueError, TypeError):
        return str(value)


async def write_sheet_data(
        df: pd.DataFrame,
        spreadsheet_id: str,
        range_name: str,
        integer_columns: Optional[List[str]] = None
) -> Optional[dict]:
    """
    Записывает данные из DataFrame в Google Sheets.

    Args:
        df: pandas DataFrame с данными для записи
        spreadsheet_id: ID таблицы Google Sheets
        range_name: Диапазон для записи (например, 'Лист1!A1:Z')
        integer_columns: Опциональный список колонок для преобразования в числа
    """
    logger.info("Запуск записи данных в Google Sheets",
                spreadsheet_id=spreadsheet_id,
                range_name=range_name,
                dataframe_shape=df.shape,
                integer_columns=integer_columns)

    # Создаем копию DataFrame для безопасной модификации
    df = df.copy()

    # Заполняем пустые значения
    df = df.fillna('')

    # Если указаны числовые колонки, обрабатываем их
    if integer_columns:
        # Проверяем существование указанных колонок
        missing_columns = [col for col in integer_columns if col not in df.columns]
        if missing_columns:
            logger.warning("Указанные числовые колонки отсутствуют в DataFrame",
                           missing_columns=missing_columns)
            integer_columns = [col for col in integer_columns if col in df.columns]

        conversion_stats = {col: {'success': 0, 'failed': 0} for col in integer_columns}

        for column in integer_columns:
            logger.debug(f"Обработка колонки: {column}")

            # Обрабатываем каждое значение в колонке
            for idx, value in df[column].items():
                original_value = value
                converted_value = safe_number_convert(value)
                df.at[idx, column] = converted_value

                # Собираем статистику конвертации
                if isinstance(converted_value, float):
                    conversion_stats[column]['success'] += 1
                    logger.debug(f"Успешное преобразование: {original_value} -> {converted_value}")
                elif str(original_value).strip():  # Только для непустых значений
                    conversion_stats[column]['failed'] += 1
                    logger.info(
                        f"Не удалось преобразовать значение в число",
                        column=column,
                        row_index=idx,
                        original_value=original_value
                    )

                    # Выводим статистику конвертации
        for col, stats in conversion_stats.items():
            logger.info(f"Статистика конвертации для колонки {col}:",
                        successful_conversions=stats['success'],
                        failed_conversions=stats['failed'])

            # Преобразуем все оставшиеся значения в строки
    for col in df.columns:
        if col not in (integer_columns or []):
            df[col] = df[col].astype(str)

    try:
        creds = await get_credentials()
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)

        # Преобразуем DataFrame в список списков
        data = df.values.tolist()

        body = {
            "values": data,
            "majorDimension": "ROWS"
        }

        request = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",  # Изменено для корректной обработки чисел
            body=body
        )

        logger.debug("Подготовлен запрос к API",
                     rows=len(data),
                     columns=len(data[0]) if data else 0)

        # Выполняем запрос асинхронно
        response = await asyncio.to_thread(request.execute)

        logger.info("Данные успешно записаны",
                    updated_cells=response.get('updatedCells'),
                    updated_rows=response.get('updatedRows'),
                    updated_columns=response.get('updatedColumns'))

        return response

    except HttpError as err:
        error_code = err.resp.status
        error_details = {
            400: "Неверный запрос или формат данных",
            403: "Отказано в доступе. Проверьте права сервисного аккаунта",
            404: "Таблица не найдена",
            429: "Превышен лимит запросов API",
            500: "Внутренняя ошибка сервера Google",
            503: "Сервис временно недоступен"
        }

        error_message = error_details.get(error_code, "Неизвестная ошибка")

        logger.error("Ошибка при записи в Google Sheets",
                     error_code=error_code,
                     error_message=error_message,
                     error_details=str(err))

        if error_code == 429:
            logger.info("Ожидание перед повторной попыткой...")
            await asyncio.sleep(60)

        return None

    except Exception as e:
        logger.exception("Критическая ошибка при записи данных",
                         error=str(e),
                         spreadsheet_id=spreadsheet_id,
                         range_name=range_name)
        return None

    finally:
        logger.debug("Завершение операции записи")