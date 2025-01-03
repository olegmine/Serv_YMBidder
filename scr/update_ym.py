from scr.logger import logger
import asyncio
import aiohttp
import pandas as pd
import json
from typing import Dict, Any


async def update_price_ym(
        df: pd.DataFrame,
        access_token: str,
        campaign_id: str,
        offer_id_col: str,
        new_price_col: str,
        discount_base_col: str,
        marketname: str,
        username: str,
        debug: bool = False
) -> None:
    # Создаем семафор для ограничения количества параллельных запросов
    semaphore = asyncio.Semaphore(4)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in df.iterrows():
            offer_id = row[offer_id_col]
            new_price = row[new_price_col]
            discount_base = row[discount_base_col]

            try:
                discount_base = int(discount_base)
            except ValueError:
                logger.warning(
                    f"Недопустимое значение базы скидки для товара {offer_id}: {discount_base}",
                    extra={"marketname": marketname, "username": username}
                )
                discount_base = 0
                logger.warning(
                    f"Установлено значение по умолчанию для базы скидки товара {offer_id}: {discount_base}",
                    extra={"marketname": marketname, "username": username}
                )

            data = {
                "offers": [
                    {
                        "offerId": offer_id,
                        "price": {
                            "value": new_price,
                            "currencyId": "RUR",
                            "discountBase": discount_base
                        }
                    }
                ]
            }

            url = f"https://api.partner.market.yandex.ru/businesses/{campaign_id}/offer-prices/updates"
            headers = {
                "Content-Type": "application/json",
                "Api-Key": access_token
            }

            if debug:
                logger.info(
                    f"Режим отладки включен. Запрос для товара {offer_id} не будет отправлен.",
                    extra={"marketname": marketname, "username": username}
                )
                logger.info(
                    f"Данные для отправки для товара {offer_id}:",
                    extra={"marketname": marketname, "username": username}
                )
                logger.info(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    extra={"marketname": marketname, "username": username}
                )
            else:
                task = asyncio.create_task(
                    rate_limited_request(semaphore, session, url, headers, data, offer_id, marketname, username)
                )
                tasks.append(task)

        if not debug:
            # Выполняем задачи группами по 4 штуки
            await asyncio.gather(*tasks)


async def rate_limited_request(
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        url: str,
        headers: Dict[str, str],
        data: Dict[str, Any],
        offer_id: str,
        marketname: str,
        username: str
) -> None:
    async with semaphore:  # Ограничиваем количество параллельных запросов
        # Добавляем небольшую задержку между запросами
        await asyncio.sleep(0.25)
        await send_request(session, url, headers, data, offer_id, marketname, username)


async def send_request(
        session: aiohttp.ClientSession,
        url: str,
        headers: Dict[str, str],
        data: Dict[str, Any],
        offer_id: str,
        marketname: str,
        username: str
) -> None:
    try:
        async with session.post(url, headers=headers, json=data) as response:
            response_text = await response.text()
            logger.info(
                f"Полный ответ сервера для товара {offer_id}:",
                extra={"marketname": marketname, "username": username}
            )
            logger.info(
                response_text,
                extra={"marketname": marketname, "username": username}
            )

            if response.status == 200:
                try:
                    response_data = json.loads(response_text)
                    if response_data.get('success') == 0:
                        error_message = response_data.get('error', {}).get('message', 'Неизвестная ошибка')
                        logger.error(
                            f"Ошибка при обновлении цены для товара {offer_id}: {error_message}",
                            extra={"marketname": marketname, "username": username}
                        )
                    else:
                        logger.info(
                            f"Цена для товара {offer_id} успешно обновлена!",
                            extra={"marketname": marketname, "username": username}
                        )
                except json.JSONDecodeError as e:
                    logger.error(
                        f"Ошибка при разборе JSON для товара {offer_id}: {str(e)}",
                        extra={"marketname": marketname, "username": username}
                    )
            else:
                logger.error(
                    f"Ошибка при отправке цены в Яндекс.Маркет для товара {offer_id}",
                    extra={"marketname": marketname, "username": username}
                )
                logger.info(
                    f"Статус ответа: {response.status}",
                    extra={"marketname": marketname, "username": username}
                )
                logger.info(
                    f"Заголовки ответа: {response.headers}",
                    extra={"marketname": marketname, "username": username}
                )
    except aiohttp.ClientError as e:
        logger.error(
            f"Ошибка сети для товара {offer_id}: {str(e)}",
            extra={"marketname": marketname, "username": username}
        )
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка для товара {offer_id}: {str(e)}",
            extra={"marketname": marketname, "username": username}
        )


# Пример использования
async def main():
    access_token = "ACMA:D4a5OExH6Hvtcx8BxgTqv2gfIpc2E7KmTPlekqDE:43a81531"
    campaign_id = "76443469"
    df = pd.DataFrame({
        "offer_id": ["ST16000NM001G"],
        "new_price": [31500],
        "discount_base": ["46000"]
    })

    await update_price_ym(
        df=df,
        access_token=access_token,
        campaign_id=campaign_id,
        offer_id_col="offer_id",
        new_price_col="new_price",
        discount_base_col="discount_base",
        marketname="YandexMarket",
        username="TestUser",
        debug=False
    )


if __name__ == "__main__":
    asyncio.run(main())

