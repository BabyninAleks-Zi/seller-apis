import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров магазина Ozon.

    Args:
        last_id (str): Идентификатор последнего полученного товара.
        client_id (str): ID клиента для авторизации в API Ozon.
        seller_token (str): API-ключ для авторизации в API Ozon.

    Returns:
        dict: Результат запроса, содержащий список товаров и метаданные пагинации.

    Пример корректного исполнения:
        >>> get_product_list("", "id клиента", "ТОКЕН")
        >>> "items" in result
        True

    Пример некорректного исполнения:
        >>> get_product_list("", "Некорректный id", "Некорректный ТОКЕН")
        Error
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
     """Получает все артикулы товаров магазина Ozon.

    Args:
        client_id (str): ID клиента для авторизации в API Ozon.
        seller_token (str): API-ключ для авторизации в API Ozon.

    Returns:
        list: Список строк-артикулов всех товаров магазина.

    Пример корректного исполнения:
        >>> ids = get_offer_ids("id клиента", "ТОКЕН")
        
    Пример некорректного исполнения:
        >>> get_offer_ids("Некорректный id", "Некорректный ТОКЕН")
        Error
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров на Ozon через API.

    Args:
        prices (list): Список словарей с данными о ценах товаров.
        client_id (str): ID клиента для авторизации в API Ozon.
        seller_token (str): API-ключ для авторизации в API Ozon.

    Returns:
        dict: Ответ от API Ozon о результате обновления цен.

    Пример корректного исполнения:
        >>> prices = create_prices()
        >>> result = update_price(prices, "id клиента", "ТОКЕН")
        True

    Пример некорректного исполнения:
        >>> update_price([Пустой файл или неверные артикулы], "Некорректный id", "Некорректный ТОКЕН")
        Error
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров на Ozon через API.

    Args:
        stocks (list): Список словарей с данными об остатках товаров.
        client_id (str): ID клиента для авторизации в API Ozon.
        seller_token (str): API-ключ для авторизации в API Ozon.

    Returns:
        dict: Ответ от API Ozon о результате обновления остатков.

    Пример корректного исполнения:
        >>> stocks = create_stocks()
        >>> result = update_stocks(stocks, "id клиента", "ТОКЕН")
        >>> isinstance(result, dict)
        True

    Пример некорректного исполнения:
        >>> update_stocks([Неверные артикулы], "Некорректный id", "Некорректный ТОКЕН")
        Error
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл ostatki с сайта casio

    Returns:
        list: Список словарей с информацией о товарах (Артикул, Количество, Цена).

    Пример корректного исполнения:
        >>> download_stock()
    
    Пример некорректного исполнения:
        >>> download_stock()
        Сайт недоступен    
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Формирует список остатков для обновления.

    Args:
        watch_remnants (list): Список словарей с данными от поставщика.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список словарей для обновления остатков.

    Пример корректного исполнения:
        >>> remnants = [{"Артикул": "111", "Количество": "5"}]
        >>> ids = ["111", "222"]
        >>> create_stocks(remnants, ids)
        5

    Пример некорректного исполнения:
        >>> create_stocks([пустой список], [пустой список])
        None
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует список цен для обновления.

    Args:
        watch_remnants (list): Список словарей с данными от поставщика.
        offer_ids (list): Список артикулов.

    Returns:
        list: Список словарей для обновления цен.

    Пример корректного исполнения:
        >>> remnants = [{"Артикул": "111", "Цена": "12 345.67 руб."}]
        >>> ids = ["111"]
        >>> create_prices(remnants, ids)
        '12345'

    Пример некорректного исполнения:
        >>> create_prices([{"Код": "111"}], [пустой список])
        [пустой список]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строку с ценой в целочисленный строковый формат.

    Args:
        price (str): Цена в произвольном формате с разделителями.
                     
    Returns:
        str: Целочисленное представление цены без разделителей.

    Пример корректного исполнения:
        >>> price_conversion("12 345.67 руб.")
        '12345'
        >>> price_conversion("111.11")
        '111'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов"""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружает цены товаров на Ozon.

    Получает список товаров с Ozon, формирует цены и отправляет их
    частями по 1000 товаров за запрос.

    Args:
        watch_remnants (list): Данные от поставщика.
        client_id (str): ID клиента для авторизации в API Ozon.
        seller_token (str): API-ключ для Ozon.

    Returns:
        list: Список всех сформированных цен.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружает остатки товаров на Ozon.

    Args:
        watch_remnants (list): Данные от поставщика.
        client_id (str): ID клиента для авторизации в API Ozon.
        seller_token (str): API-ключ для Ozon.

    Returns:
        Кортеж: (not_empty, stocks) где:
            not_empty (list): Товары с ненулевым остатком.
            stocks (list): Все товары с остатками.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """
    Основная функция для запуска синхронизации с Ozon.

    Пример корректного исполнения:
        >>> main(): Выполнит полную синхронизацию без ошибок

    Пример некорректного исполнения:
        При отсутствии переменных окружения вызовет ошибку
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
