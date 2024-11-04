import pandas as pd
import numpy as np
import logging
from datetime import datetime
import random
import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Dict
from scr.logger import logger

# Настройка логирования
logger = logger

# Создаем глобальный ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=4)


async def run_in_executor(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, func, *args)


async def update_dataframe(df1: pd.DataFrame, df2: pd.DataFrame, column_names: Dict[str, str]) -> pd.DataFrame:
    """
    Асинхронно обновляет первый DataFrame данными из второго DataFrame на основе seller_id.

    :param df1: Первый DataFrame
    :param df2: Второй DataFrame
    :param column_names: Словарь с названиями колонок
    :return: Обновленный DataFrame
    """

    def update_df():
        df1_updated = df1.copy()
        df2_updated = df2.copy()

        seller_id = column_names['seller_id']
        mp_on_market = column_names['mp_on_market']
        market_with_mp = column_names['market_with_mp']

        df1_updated[seller_id] = df1_updated[seller_id].astype(str)
        df2_updated[seller_id] = df2_updated[seller_id].astype(str)

        merged_df = df1_updated.merge(df2_updated[[seller_id, mp_on_market, market_with_mp]],
                                      on=seller_id,
                                      how='left',
                                      suffixes=('', '_new'))

        merged_df[mp_on_market] = merged_df[f'{mp_on_market}_new'].fillna(merged_df[mp_on_market])
        merged_df[market_with_mp] = merged_df[f'{market_with_mp}_new'].fillna(merged_df[market_with_mp])

        merged_df = merged_df.drop([f'{mp_on_market}_new', f'{market_with_mp}_new'], axis=1)

        original_type = df1[seller_id].dtype
        merged_df[seller_id] = merged_df[seller_id].astype(original_type)

        return merged_df

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
        updated_df.loc[empty_stop_mask, column_names['prim']] = "Пустое значение в колонке 'stop'"

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


        # async def main():
#     # Определение названий колонок
#     column_names = {
#         'seller_id': 'SHOP_SKU',
#         'name': 'OFFER',
#         'link': 'LINK',
#         'price': 'MERCH_PRICE_WITH_PROMOS',
#         'stop': 'STOP',
#         'mp_on_market': 'PRICE.1',
#         'market_with_mp': 'SHOP_WITH_BEST_PRICE_ON_MARKET',
#         'prim': 'PRIM'
#     }
#
#     # Пример использования:
#     df1 = pd.read_csv('report_old.csv')
#     df1['LINK'] =''
#     df2 = pd.read_csv('report_old — копия.csv')
#
#
#     # Обновляем df1 данными из df2
#     updated_df = await update_dataframe(df1, df2, column_names)
#     print("Обновленный DataFrame:")
#     print(updated_df)
#     print("\nТипы данных:")
#     print(updated_df.dtypes)
#
#     # Создаем DataFrame for_update и логируем случаи, когда mp_on_market ниже stop
#     updated_df, for_update_df = await compare_prices_and_create_for_update(updated_df, column_names)
#     for_update_df.to_csv('for_updated.csv')
#     updated_df.to_csv('updated.csv')
#     print("\nDataFrame for_update:")
#     print(for_update_df)
#     print(updated_df)
#
#     await run_in_executor(for_update_df.to_csv, 'report/reported2.txt')
#
#     df = await run_in_executor(pd.read_csv, 'report/reported22.txt')
#     print(df.info())


# if __name__ == "__main__":
#     asyncio.run(main())
