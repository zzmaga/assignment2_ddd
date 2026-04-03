"""
QazaqPrice — Парсер данных для Assignment №2
Источники: sulpak.kz и mechta.kz
Цель: собрать 2500+ записей о товарах электроники в Казахстане

Требования: pip install requests pandas tqdm
"""

import requests
import pandas as pd
import time
import json
import random
from tqdm import tqdm

# ─────────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9",
}

DELAY_MIN = 0.5  # секунды между запросами (минимум)
DELAY_MAX = 1.5  # секунды между запросами (максимум)


def sleep():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


# ─────────────────────────────────────────────
# ИСТОЧНИК 1: SULPAK.KZ
# API каталога (публичный, без авторизации)
# ─────────────────────────────────────────────

SULPAK_CATEGORIES = {
    "smartphones": "smartfony",
    "laptops": "noutbuki",
    "tablets": "planshety",
    "tvs": "televizory",
    "headphones": "naushniki",
    "cameras": "fotoapparaty",
    "smart_watches": "umnye-chasy",
    "game_consoles": "igrovye-pristavki",
}


def scrape_sulpak(max_per_category=200):
    """
    Парсит sulpak.kz через публичный REST API каталога.
    Возвращает DataFrame.
    """
    records = []

    for cat_name, cat_slug in SULPAK_CATEGORIES.items():
        print(f"\n[Sulpak] Категория: {cat_name}")
        page = 1
        collected = 0

        while collected < max_per_category:
            url = (
                f"https://www.sulpak.kz/api/v1/catalog/products"
                f"?categorySlug={cat_slug}"
                f"&page={page}&perPage=48&sort=popularity"
            )
            try:
                resp = requests.get(url, headers=HEADERS, timeout=15)
                if resp.status_code != 200:
                    print(f"  ⚠ HTTP {resp.status_code} — пропускаем страницу {page}")
                    break

                data = resp.json()
                items = data.get("data", data.get("products", data.get("items", [])))

                if not items:
                    break

                for item in items:
                    price_raw = item.get("price") or item.get("currentPrice") or 0
                    try:
                        price = float(
                            str(price_raw).replace(" ", "").replace("\u00a0", "")
                        )
                    except Exception:
                        price = None

                    records.append(
                        {
                            "source": "sulpak.kz",
                            "category": cat_name,
                            "product_id": item.get("id") or item.get("sku"),
                            "product_name": item.get("name") or item.get("title"),
                            "brand": item.get("brand") or item.get("brandName"),
                            "price_kzt": price,
                            "old_price_kzt": item.get("oldPrice")
                            or item.get("previousPrice"),
                            "rating": item.get("rating") or item.get("reviewRating"),
                            "reviews_count": item.get("reviewsCount")
                            or item.get("reviewCount")
                            or 0,
                            "in_stock": item.get("inStock", True),
                            "url": f"https://www.sulpak.kz/g/{item.get('slug', '')}",
                        }
                    )
                    collected += 1

                print(f"  Страница {page}: +{len(items)} товаров (итого {collected})")
                page += 1
                sleep()

                # Если товаров на странице меньше pageSize — это последняя страница
                if len(items) < 48:
                    break

            except Exception as e:
                print(f"  ✗ Ошибка: {e}")
                break

    df = pd.DataFrame(records)
    print(f"\n[Sulpak] Итого: {len(df)} записей")
    return df


# ─────────────────────────────────────────────
# ИСТОЧНИК 2: MECHTA.KZ
# Публичный API каталога
# ─────────────────────────────────────────────

MECHTA_CATEGORIES = {
    "smartphones": "smartfony",
    "laptops": "noutbuki",
    "tablets": "planshety",
    "tvs": "televizory",
    "headphones": "naushniki",
    "cameras": "fotoapparaty-i-videokamery",
    "smart_watches": "smart-chasy",
    "accessories": "aksessuary-dlya-telefonov",
}


def scrape_mechta(max_per_category=200):
    """
    Парсит mechta.kz через публичный REST API каталога.
    Возвращает DataFrame.
    """
    records = []

    for cat_name, cat_slug in MECHTA_CATEGORIES.items():
        print(f"\n[Mechta] Категория: {cat_name}")
        page = 1
        collected = 0

        while collected < max_per_category:
            url = (
                f"https://www.mechta.kz/api/v2/catalog/products"
                f"?category={cat_slug}"
                f"&page={page}&limit=48"
            )
            try:
                resp = requests.get(url, headers=HEADERS, timeout=15)
                if resp.status_code != 200:
                    print(
                        f"  ⚠ HTTP {resp.status_code} — пробуем альтернативный endpoint"
                    )
                    # Fallback: попробовать другой формат URL
                    url2 = (
                        f"https://www.mechta.kz/api/catalog"
                        f"?slug={cat_slug}&page={page}&per_page=48"
                    )
                    resp = requests.get(url2, headers=HEADERS, timeout=15)
                    if resp.status_code != 200:
                        break

                data = resp.json()
                items = (
                    data.get("products")
                    or data.get("data", {}).get("products")
                    or data.get("items")
                    or []
                )

                if not items:
                    break

                for item in items:
                    price_raw = (
                        item.get("price")
                        or item.get("currentPrice")
                        or item.get("priceValue")
                        or 0
                    )
                    try:
                        price = float(
                            str(price_raw).replace(" ", "").replace("\u00a0", "")
                        )
                    except Exception:
                        price = None

                    records.append(
                        {
                            "source": "mechta.kz",
                            "category": cat_name,
                            "product_id": item.get("id")
                            or item.get("sku")
                            or item.get("article"),
                            "product_name": item.get("name") or item.get("title"),
                            "brand": item.get("brand") or item.get("manufacturer"),
                            "price_kzt": price,
                            "old_price_kzt": item.get("oldPrice")
                            or item.get("priceOld"),
                            "rating": item.get("rating") or item.get("stars"),
                            "reviews_count": item.get("reviewsCount")
                            or item.get("reviews")
                            or 0,
                            "in_stock": item.get("inStock", True),
                            "url": f"https://www.mechta.kz/product/{item.get('slug', item.get('id', ''))}",
                        }
                    )
                    collected += 1

                print(f"  Страница {page}: +{len(items)} товаров (итого {collected})")
                page += 1
                sleep()

                if len(items) < 48:
                    break

            except Exception as e:
                print(f"  ✗ Ошибка: {e}")
                break

    df = pd.DataFrame(records)
    print(f"\n[Mechta] Итого: {len(df)} записей")
    return df


# ─────────────────────────────────────────────
# ЕСЛИ API НЕ РАБОТАЕТ — РЕЗЕРВНЫЙ ИСТОЧНИК
# Используем публичный Open Dataset с данными
# казахстанских магазинов (dataset.kz / data.egov.kz)
# ─────────────────────────────────────────────


def scrape_fallback_via_search():
    """
    Резервный вариант: парсим через поисковые запросы
    к публичному API Sulpak (другой endpoint).
    """
    records = []
    brands = [
        "Samsung",
        "Apple",
        "Xiaomi",
        "Huawei",
        "Realme",
        "OPPO",
        "Honor",
        "Lenovo",
        "Acer",
        "ASUS",
        "Sony",
        "LG",
        "Philips",
        "TCL",
        "Hisense",
    ]
    categories = ["смартфон", "ноутбук", "телевизор", "планшет", "наушники"]

    for brand in brands:
        for cat in categories:
            query = f"{brand} {cat}"
            url = f"https://www.sulpak.kz/api/v1/search?q={requests.utils.quote(query)}&limit=50"
            try:
                resp = requests.get(url, headers=HEADERS, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    items = (
                        data.get("products")
                        or data.get("data")
                        or data.get("results")
                        or []
                    )
                    for item in items:
                        price_raw = item.get("price") or item.get("currentPrice") or 0
                        try:
                            price = float(str(price_raw).replace(" ", ""))
                        except Exception:
                            price = None
                        records.append(
                            {
                                "source": "sulpak.kz",
                                "category": cat,
                                "product_id": item.get("id"),
                                "product_name": item.get("name") or item.get("title"),
                                "brand": brand,
                                "price_kzt": price,
                                "old_price_kzt": item.get("oldPrice"),
                                "rating": item.get("rating"),
                                "reviews_count": item.get("reviewsCount") or 0,
                                "in_stock": item.get("inStock", True),
                                "url": f"https://www.sulpak.kz/g/{item.get('slug', '')}",
                            }
                        )
                sleep()
            except Exception as e:
                print(f"  Ошибка поиска '{query}': {e}")

    return pd.DataFrame(records)


# ─────────────────────────────────────────────
# ОБЪЕДИНЕНИЕ И ОЧИСТКА
# ─────────────────────────────────────────────


def clean_and_merge(df_sulpak, df_mechta):
    """
    Объединяет датасеты, очищает, убирает дубликаты, выбросы.
    """
    df = pd.concat([df_sulpak, df_mechta], ignore_index=True)
    print(f"\n[Merge] До очистки: {len(df)} записей")

    # 1. Убираем строки без названия товара
    df = df[df["product_name"].notna() & (df["product_name"] != "")]

    # 2. Убираем дубликаты (по источнику + id или по названию+цена)
    before = len(df)
    df = df.drop_duplicates(subset=["source", "product_id"], keep="first")
    df = df.drop_duplicates(
        subset=["product_name", "price_kzt", "source"], keep="first"
    )
    print(f"[Clean] Удалено дубликатов: {before - len(df)}")

    # 3. Убираем строки с нулевой или отсутствующей ценой
    before = len(df)
    df = df[df["price_kzt"].notna() & (df["price_kzt"] > 0)]
    print(f"[Clean] Удалено записей без цены: {before - len(df)}")

    # 4. Выбросы по цене методом 1.5×IQR
    before = len(df)
    Q1 = df["price_kzt"].quantile(0.25)
    Q3 = df["price_kzt"].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    outliers_mask = (df["price_kzt"] < lower) | (df["price_kzt"] > upper)
    df["is_outlier"] = outliers_mask
    df_clean = df[~outliers_mask].copy()
    print(
        f"[Clean] Выбросов по цене (1.5×IQR): {outliers_mask.sum()} "
        f"(bounds: {lower:,.0f} – {upper:,.0f} KZT)"
    )
    print(f"[Clean] После удаления выбросов: {len(df_clean)} записей")

    # 5. Нормализация текста
    df_clean["product_name"] = df_clean["product_name"].str.strip()
    df_clean["brand"] = (
        df_clean["brand"].str.strip().str.title() if "brand" in df_clean.columns else ""
    )

    # 6. Заполняем пропуски в рейтинге
    df_clean["rating"] = pd.to_numeric(df_clean["rating"], errors="coerce")
    df_clean["reviews_count"] = (
        pd.to_numeric(df_clean["reviews_count"], errors="coerce").fillna(0).astype(int)
    )

    # 7. Ценовой сегмент
    def price_segment(p):
        if p < 50_000:
            return "low-priced"
        elif p < 200_000:
            return "middle-priced"
        elif p < 500_000:
            return "high-priced"
        else:
            return "luxury"

    df_clean["price_segment"] = df_clean["price_kzt"].apply(price_segment)

    print(f"\n[Clean] Итоговый датасет: {len(df_clean)} записей")
    print(df_clean["source"].value_counts().to_string())
    print(df_clean["category"].value_counts().to_string())

    return df_clean, df[outliers_mask]  # возвращаем и выбросы для документирования


# ─────────────────────────────────────────────
# СОХРАНЕНИЕ
# ─────────────────────────────────────────────


def save_all(df_clean, df_outliers):
    # CSV
    df_clean.to_csv("qazaqprice_dataset.csv", index=False, encoding="utf-8-sig")
    print("✓ Сохранено: qazaqprice_dataset.csv")

    # JSON
    df_clean.to_json(
        "qazaqprice_dataset.json", orient="records", force_ascii=False, indent=2
    )
    print("✓ Сохранено: qazaqprice_dataset.json")

    # XML
    with open("qazaqprice_dataset.xml", "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<products>\n')
        for _, row in df_clean.iterrows():
            f.write("  <product>\n")
            for col in df_clean.columns:
                val = (
                    str(row[col])
                    .replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                )
                f.write(f"    <{col}>{val}</{col}>\n")
            f.write("  </product>\n")
        f.write("</products>")
    print("✓ Сохранено: qazaqprice_dataset.xml")

    # Лог изменений
    with open("data_cleaning_log.txt", "w", encoding="utf-8") as f:
        f.write("=== DATA CLEANING LOG — QazaqPrice Dataset ===\n\n")
        f.write(f"Источники: sulpak.kz, mechta.kz\n")
        f.write(f"Дата сбора: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write(f"Итоговых записей: {len(df_clean)}\n")
        f.write(f"Выбросов удалено: {len(df_outliers)}\n\n")
        f.write("Распределение по источникам:\n")
        f.write(df_clean["source"].value_counts().to_string())
        f.write("\n\nРаспределение по категориям:\n")
        f.write(df_clean["category"].value_counts().to_string())
        f.write("\n\nЦеновые сегменты:\n")
        f.write(df_clean["price_segment"].value_counts().to_string())
        f.write("\n\nСтатистика по ценам (KZT):\n")
        f.write(df_clean["price_kzt"].describe().to_string())
    print("✓ Сохранено: data_cleaning_log.txt")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("QazaqPrice — Сбор данных для Assignment №2")
    print("=" * 60)

    # Парсим оба источника
    print("\n>>> Парсинг Sulpak.kz...")
    df_sulpak = scrape_sulpak(max_per_category=200)

    # Если Sulpak не дал данных — используем fallback
    if len(df_sulpak) < 100:
        print("  Основной API не ответил, пробуем fallback через поиск...")
        df_sulpak = scrape_fallback_via_search()

    print("\n>>> Парсинг Mechta.kz...")
    df_mechta = scrape_mechta(max_per_category=200)

    # Объединяем и чистим
    print("\n>>> Объединение и очистка...")
    df_clean, df_outliers = clean_and_merge(df_sulpak, df_mechta)

    # Сохраняем
    print("\n>>> Сохранение файлов...")
    save_all(df_clean, df_outliers)

    print("\n✅ Готово!")
    print(f"   Итого в датасете: {len(df_clean)} записей")
    print(f"   Атрибутов: {len(df_clean.columns)}")
    print(f"\n   Файлы: qazaqprice_dataset.csv / .json / .xml")
    print(f"          data_cleaning_log.txt")
