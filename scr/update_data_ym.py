import pandas as pd
import numpy as np
import logging
from datetime import datetime
import random
import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple, List
from scr.logger import logger

# Настройка логирования
logger = logger

# Создаем глобальный ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=4)


async def run_in_executor(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, func, *args)


async def update_dataframe(df1: pd.DataFrame, df2: pd.DataFrame, column_names: Dict[str, str], user_name: str,
                           market_name: str) -> pd.DataFrame:
    """
    Асинхронно обновляет первый DataFrame данными из второго DataFrame.
    """
    def update_df():
        def standardize_shop_sku(value):
            """Стандартизация значений SHOP_SKU"""
            if pd.isna(value):
                return value
            # Преобразуем в строку и убираем пробелы
            str_value = str(value).strip()
            # Удаляем ведущие нули и снова приводим к строке
            str_value = str(int(str_value)) if str_value.isdigit() else str_value
            return str_value

        try:
            # Сбрасываем индекс, если он не является значимым
            df1_updated = df1.reset_index(drop=True) if not isinstance(df1.index, pd.MultiIndex) else df1.copy()
            df2_updated = df2.reset_index(drop=True) if not isinstance(df2.index, pd.MultiIndex) else df2.copy()

            # Логируем начальное состояние
            logger.debug(
                f"Исходные значения SHOP_SKU в df1:\n{df1_updated[['SHOP_SKU', 'OFFER']].to_dict('records')}",
                extra={'user_name': user_name, 'market_name': market_name}
            )

            # Стандартизируем SHOP_SKU
            df1_updated['SHOP_SKU'] = df1_updated['SHOP_SKU'].apply(standardize_shop_sku)
            df2_updated['SHOP_SKU'] = df2_updated['SHOP_SKU'].apply(standardize_shop_sku)

            # Логируем состояние после стандартизации
            logger.debug(
                f"Значения SHOP_SKU после стандартизации:\n{df1_updated[['SHOP_SKU', 'OFFER']].to_dict('records')}",
                extra={'user_name': user_name, 'market_name': market_name}
            )

            # Проверяем дубликаты до слияния
            duplicates_df1 = df1_updated[df1_updated.duplicated(subset=['SHOP_SKU'], keep=False)]
            if not duplicates_df1.empty:
                logger.warning(
                    f"Найдены дубликаты в первом DataFrame:\n{duplicates_df1[['SHOP_SKU', 'OFFER']].to_dict('records')}",
                    extra={'user_name': user_name, 'market_name': market_name}
                )
                # Оставляем только первую запись для каждого дубликата
                df1_updated = df1_updated.drop_duplicates(subset=['SHOP_SKU'], keep='first')

            # Определяем колонки для обновления
            update_columns = [
                column_names[key] for key in [
                    'name', 'link', 'price', 'stop', 'mp_on_market',
                    'market_with_mp', 'prim'
                ] if key in column_names and column_names[key] in df2_updated.columns
            ]

            # Добавляем дополнительные колонки
            additional_columns = [
                'PRICE_GREEN_THRESHOLD',
                'PRICE_RED_THRESHOLD'
            ]
            update_columns.extend([
                col for col in additional_columns
                if col in df2_updated.columns
            ])

            # Выполняем слияние
            merged_df = pd.merge(
                df1_updated,
                df2_updated[['SHOP_SKU'] + update_columns],
                on='SHOP_SKU',
                how='outer',
                suffixes=('', '_new')
            )

            # Обновляем значения
            for col in update_columns:
                if f'{col}_new' in merged_df.columns:
                    merged_df[col] = merged_df[f'{col}_new'].fillna(merged_df[col])
                    merged_df = merged_df.drop(columns=[f'{col}_new'])

            # Заполняем пустые значения
            merged_df = merged_df.fillna(pd.NA)

            # Финальная проверка на дубликаты
            final_duplicates = merged_df.duplicated(subset=['SHOP_SKU'], keep=False)
            if final_duplicates.any():
                logger.error(
                    f"Финальные дубликаты:\n{merged_df[final_duplicates][['SHOP_SKU', 'OFFER']].to_dict('records')}",
                    extra={'user_name': user_name, 'market_name': market_name}
                )
                merged_df = merged_df.drop_duplicates(subset=['SHOP_SKU'], keep='first')

            # Проверяем финальное состояние
            logger.debug(
                f"Финальное состояние:\n{merged_df[['SHOP_SKU', 'OFFER']].to_dict('records')}\n"  
                f"Количество уникальных SHOP_SKU: {merged_df['SHOP_SKU'].nunique()}",
                extra={'user_name': user_name, 'market_name': market_name}
            )

            return merged_df

        except Exception as e:
            logger.error(
                f"Ошибка при обработке DataFrame: {str(e)}\n"  
                f"Текущее состояние данных:\n"  
                f"df1 shape: {df1.shape}\n"  
                f"df2 shape: {df2.shape}",
                extra={'user_name': user_name, 'market_name': market_name}
            )
            raise

    return await run_in_executor(update_df)

async def compare_prices_and_create_for_update(
        df: pd.DataFrame,
        column_names: Dict[str, str],
        my_market: list[str],
        db_file: str,
        username: str,
        marketname: str,
        min_price_diff: int = 50,
        max_price_diff: int = 200
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Асинхронно сравнивает цены и создает DataFrame для обновления.

    Args:
        df: Исходный DataFrame
        column_names: Словарь с названиями колонок
        my_market: Список названий ваших магазинов
        db_file: Путь к файлу базы данных SQLite
        username: Имя пользователя
        marketname: Название магазина
        min_price_diff: Минимальная разница в цене
        max_price_diff: Максимальная разница в цене
    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Обновленный DataFrame и DataFrame для обновления
    """

    async def save_price_changes_to_db(changes_df: pd.DataFrame, is_successful: bool = True) -> None:
        """
        Сохраняет изменения цен в базу данных SQLite.

        Args:
            changes_df: DataFrame с изменениями цен
            is_successful: Флаг успешности изменения цены
        """
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Создаем таблицу для успешных изменений
            cursor.execute("""  
            CREATE TABLE IF NOT EXISTS price_change_history (  
                id INTEGER PRIMARY KEY AUTOINCREMENT,  
                seller_id TEXT,  
                old_price REAL,  
                new_price REAL,  
                mp_on_market REAL,  
                stop_price REAL,  
                discount_base REAL,  
                change_date TEXT,  
                username TEXT,  
                marketname TEXT,  
                change_reason TEXT  
            )  
            """)

            # Создаем таблицу для неуспешных попыток изменения
            cursor.execute("""  
            CREATE TABLE IF NOT EXISTS failed_price_change_attempts (  
                id INTEGER PRIMARY KEY AUTOINCREMENT,  
                seller_id TEXT,  
                current_price REAL,  
                mp_on_market REAL,  
                stop_price REAL,  
                attempt_date TEXT,  
                username TEXT,  
                marketname TEXT,  
                failure_reason TEXT,  
                market_with_mp TEXT  
            )  
            """)

            if is_successful:
                # Записываем успешные изменения
                for _, row in changes_df.iterrows():
                    cursor.execute("""  
                    INSERT INTO price_change_history (  
                        seller_id, old_price, new_price, mp_on_market,  
                        stop_price, discount_base, change_date,  
                        username, marketname, change_reason  
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  
                    """, (
                        str(row[column_names['seller_id']]),
                        float(row['old_price']),
                        float(row[column_names['price']]),
                        float(row[column_names['mp_on_market']]),
                        float(row[column_names['stop']]),
                        float(row.get('discount_base', 0)),
                        datetime.now().isoformat(),
                        username,
                        marketname,
                        str(row[column_names['prim']])
                    ))
            else:
                # Записываем неуспешные попытки
                for _, row in changes_df.iterrows():
                    cursor.execute("""  
                    INSERT INTO failed_price_change_attempts (  
                        seller_id, current_price, mp_on_market, stop_price,  
                        attempt_date, username, marketname, failure_reason,  
                        market_with_mp  
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)  
                    """, (
                        str(row[column_names['seller_id']]),
                        float(row[column_names['price']]),
                        float(row[column_names['mp_on_market']]),
                        float(row[column_names['stop']]),
                        datetime.now().isoformat(),
                        username,
                        marketname,
                        str(row[column_names['prim']]),
                        str(row[column_names['market_with_mp']])
                    ))

            conn.commit()
            conn.close()
            logger.info(
                f"Успешно сохранены {'изменения' if is_successful else 'неуспешные попытки изменения'} цен в базу данных",
                username=username,
                marketname=marketname
            )

        except sqlite3.Error as e:
            logger.error(
                f"Ошибка при работе с БД SQLite: {str(e)}",
                username=username,
                marketname=marketname
            )
            raise

    async def calculate_new_price(row, min_price_diff: int = 50, max_price_diff: int = 200):
        """
        Рассчитывает новую цену товара с учетом заданных диапазонов изменения цены.
        """
        try:
            old_price = row[column_names['price']]
            mp_on_market = row[column_names['mp_on_market']]
            stop = row[column_names['stop']]
            shop_with_best_price = row[column_names['market_with_mp']]

            if mp_on_market - old_price > max_price_diff:
                preliminary_price = mp_on_market - random.randint(min_price_diff, max_price_diff)
                new_price = max(preliminary_price, stop)

                if new_price == stop:
                    message = (
                        f"Цена была значительно ниже рыночной ({old_price:.2f} << {mp_on_market:.2f}). "
                        f"Расчетная цена была ниже stop, установлена в stop: {new_price:.2f}"
                    )
                else:
                    message = (
                        f"Цена была значительно ниже рыночной ({old_price:.2f} << {mp_on_market:.2f}). "
                        f"Увеличена до {new_price:.2f} (mp_on_market - случайное число от {min_price_diff} до {max_price_diff})"
                    )

                new_discount_base = round(random.uniform(new_price * 1.3, new_price * 1.6), 0)
                return (
                    new_price,
                    f"{message}. Новая discount_base: {new_discount_base:.2f}",
                    new_discount_base
                )

            # Новая логика: если текущая цена ниже stop
            if old_price < stop:
                price_increase = random.randint(20, 50)
                new_price = stop + price_increase
                new_discount_base = round(random.uniform(new_price * 1.3, new_price * 1.6), 0)
                return (
                    new_price,
                    f"Цена была ниже stop ({old_price:.2f} < {stop:.2f}). "
                    f"Увеличена до stop + {price_increase} = {new_price:.2f}. "
                    f"Новая discount_base: {new_discount_base:.2f}",
                    new_discount_base
                )
            else:
                if shop_with_best_price in my_market:
                    return old_price, f"Цена не изменена. У одного из ваших магазинов ({shop_with_best_price}) уже минимальная цена на рынке.", None

                price_difference = old_price - mp_on_market

                if price_difference > max_price_diff:
                    min_new_price = max(mp_on_market - max_price_diff, stop)
                    max_new_price = mp_on_market - min_price_diff
                else:
                    min_new_price = max(mp_on_market - min_price_diff, stop)
                    max_new_price = mp_on_market - 1

                if min_new_price >= max_new_price:
                    return old_price, (
                        f"Цена не изменена. Некорректный диапазон цен. "
                        f"Текущая цена: {old_price:.2f}, mp_on_market: {mp_on_market:.2f}, "
                        f"stop: {stop:.2f}"
                    ), None

                new_price = max(random.randint(int(min_new_price), int(max_new_price)), int(stop))
                new_discount_base = round(random.uniform(new_price * 1.3, new_price * 1.6), 0)

                return (
                    new_price,
                    f"Цена изменена с {old_price:.2f} на {new_price:.2f}. "
                    f"Новая discount_base: {new_discount_base:.2f} "
                    f"(mp_on_market: {mp_on_market:.2f}, "
                    f"диапазон: {min_new_price:.2f} - {max_new_price:.2f})",
                    new_discount_base
                )

        except Exception as e:
            logger.error(
                f"Ошибка при расчете новой цены для товара {row[column_names['seller_id']]}: {str(e)}",
                username=username,
                marketname=marketname
            )
            return row[column_names['price']], f"Ошибка при расчете новой цены: {str(e)}", None

    try:
        updated_df = df.copy()
        updated_df[column_names['prim']] = ''
        updated_df['old_price'] = updated_df[column_names['price']]

        # Создаем DataFrame для хранения неуспешных попыток
        failed_attempts_df = pd.DataFrame()

        numeric_columns = [column_names['price'], column_names['mp_on_market'], column_names['stop']]
        for col in numeric_columns:
            updated_df[col] = pd.to_numeric(updated_df[col], errors='coerce')

            # Проверка на пустые значения в колонке 'stop'
        empty_stop_mask = updated_df[column_names['stop']].isna()
        updated_df.loc[empty_stop_mask, column_names['prim']] = "Пустое значение в колонке 'STOP' данный товар исключен из обработки Биддера"

        # Добавляем записи с пустыми stop в failed_attempts
        if empty_stop_mask.any():
            failed_attempts_df = pd.concat([
                failed_attempts_df,
                updated_df[empty_stop_mask].copy()
            ])

        nan_mask = updated_df[numeric_columns].isna().any(axis=1)
        if nan_mask.any():
            logger.warning(
                f"Обнаружены NaN значения в {nan_mask.sum()} строках",
                username=username,
                marketname=marketname
            )
            # Добавляем записи с NaN значениями в failed_attempts
            failed_attempts_df = pd.concat([
                failed_attempts_df,
                updated_df[nan_mask].copy()
            ])

        mask = (
                (updated_df[column_names['price']] > updated_df[column_names['mp_on_market']]) &
                (updated_df[column_names['mp_on_market']] > updated_df[column_names['stop']]) &
                (updated_df[column_names['price']] < updated_df[column_names['stop']]) |
                (~empty_stop_mask)
        )

        # Добавляем записи, не удовлетворяющие условиям изменения
        failed_conditions_mask = ~mask & ~empty_stop_mask & ~nan_mask
        if failed_conditions_mask.any():
            failed_df = updated_df[failed_conditions_mask].copy()
            failed_df[column_names['prim']] = "Не удовлетворяет условиям изменения цены"
            failed_attempts_df = pd.concat([failed_attempts_df, failed_df])

        results = await asyncio.gather(
            *[calculate_new_price(row, min_price_diff=min_price_diff, max_price_diff=max_price_diff)
              for _, row in updated_df[mask].iterrows()]
        )

        price_changed = pd.Series(False, index=updated_df.index)

        if results:
            new_prices, new_prims, new_discount_bases = zip(*results)
            price_changed[mask] = [
                new_price != old_price
                for new_price, old_price in zip(new_prices, updated_df.loc[mask, column_names['price']])
            ]

            updated_df.loc[mask, column_names['price']] = new_prices
            updated_df.loc[mask, column_names['prim']] = new_prims

            # Добавляем в failed_attempts случаи, где цена не изменилась
            no_change_mask = mask & ~price_changed
            if no_change_mask.any():
                failed_attempts_df = pd.concat([
                    failed_attempts_df,
                    updated_df[no_change_mask].copy()
                ])

            discount_base_mask = mask & price_changed
            updated_df.loc[discount_base_mask, 'discount_base'] = [
                db for db, changed in zip(new_discount_bases, price_changed[mask])
                if changed and db is not None
            ]
        else:
            logger.info(
                "Нет строк для обновления цен",
                username=username,
                marketname=marketname
            )

        for index, row in updated_df.iterrows():
            if not pd.isna(row[column_names['mp_on_market']]) and not pd.isna(row[column_names['stop']]):
                if row[column_names['mp_on_market']] <= row[column_names['stop']]:
                    warning_msg = (
                        f"Оптимальная цена mp_on_market ({row[column_names['mp_on_market']]:.2f}) "
                        f"ниже или равна минимальной stop ({row[column_names['stop']]:.2f})"
                    )
                    logger.info(warning_msg, username=username, marketname=marketname)
                    updated_df.loc[index, column_names['prim']] = warning_msg
                    # Добавляем в failed_attempts
                    failed_attempts_df = pd.concat([
                        failed_attempts_df,
                        updated_df.loc[[index]].copy()
                    ])

                    # Создаем for_update только для товаров с измененными ценами
        for_update = updated_df[price_changed].copy()

        try:
            updated_df = updated_df.drop('discount_base', axis=1)
        except:
            logger.info(
                "Колонка 'discount_base' отсутствует в DataFrame updated_df",
                username=username,
                marketname=marketname
            )
        try:
            updated_df = updated_df.drop('old_price', axis=1)
        except:
            logger.info(
                "Колонка 'old_price' отсутствует в DataFrame updated_df",
                username=username,
                marketname=marketname
            )

        if 'discount_base' not in for_update.columns:
            logger.info(
                "Колонка 'discount_base' отсутствует в DataFrame for_update",
                username=username,
                marketname=marketname
            )

            # Сохраняем неуспешные попытки в базу данных
        if not failed_attempts_df.empty:
            await save_price_changes_to_db(failed_attempts_df, is_successful=False)
            logger.info(
                f"Сохранены данные о {len(failed_attempts_df)} неуспешных попытках изменения цен",
                username=username,
                marketname=marketname
            )

            # Сохраняем успешные изменения в базу данных
        if len(for_update) > 0:
            await save_price_changes_to_db(for_update, is_successful=True)
            logger.info(
                f"Сохранены изменения цен для {len(for_update)} товаров",
                username=username,
                marketname=marketname
            )

        return updated_df, for_update

    except Exception as e:
        logger.error(
            f"Критическая ошибка при обработке данных: {str(e)}",
            username=username,
            marketname=marketname
        )
        raise


import pandas as pd
import asyncio


async def first_write_df(df: pd.DataFrame) -> pd.DataFrame:
    # Словарь с описаниями колонок
    column_descriptions = {
        'SHOP_SKU': 'Идентификатор товара в магазине',
        'OFFER': 'Название Предложения',
        'MERCH_PRICE_WITH_PROMOS': 'Цена с учетом промо-акций продавца',
        'PRICE_GREEN_THRESHOLD': 'Зеленый порог цены',
        'PRICE_RED_THRESHOLD': 'Красный порог цены',
        'SHOP_WITH_BEST_PRICE_ON_MARKET': 'Магазин с лучшей ценой на рынке',
        'PRICE_VALUE_ON_MARKET': 'Значение цены на рынке',
        'PRIM': 'Примечание',
        'LINK': 'Ссылка на товар',
        'STOP': 'Ограничитель цены'
    }

    # Порядок колонок
    column_order = [
        'SHOP_SKU',
        'OFFER',
        'LINK',
        'MERCH_PRICE_WITH_PROMOS',
        'PRICE_GREEN_THRESHOLD',
        'PRICE_RED_THRESHOLD',
        'SHOP_WITH_BEST_PRICE_ON_MARKET',
        'PRICE_VALUE_ON_MARKET',
        'STOP',
        'PRIM'
    ]

    # Создаем копию датафрейма
    new_df = df.copy()

    # Добавляем новую колонку prim
    new_df['PRIM'] = 'Нет значения'
    new_df['LINK'] = 'Нет значения'
    new_df['STOP'] = 'Нет значения'

    # Создаем строку с описаниями
    descriptions = pd.DataFrame([column_descriptions])

    # Объединяем описания с данными
    result_df = pd.concat([descriptions, new_df], ignore_index=True)

    # Переупорядочиваем колонки
    result_df = result_df[column_order]

    return result_df