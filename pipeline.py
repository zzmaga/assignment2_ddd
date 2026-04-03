"""
QazaqPrice — Assignment №2: сбор и очистка данных (2+ источника, ≥2500 строк).

Источники (открытые страницы, Казахстан):
  1) sulpak.kz — карточки товаров (цена в data-price, название в <title>)
  2) satu.kz — JSON-LD schema.org/Product (цена в KZT, продавец, состояние)

Запуск:
  python pipeline.py                      # --raw-total 3000 по умолчанию
  python pipeline.py --quick              # быстрая проверка (~120 строк)
  python pipeline.py --raw-total 2800       # объём сырья до очистки (сумма двух источников)
"""

from __future__ import annotations

import argparse
import json
import random
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,kk;q=0.8,en-US;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SULPAK_SITEMAP = "https://www.sulpak.kz/sitemap-almaty-ru.xml"

SULPAK_CATEGORY_HINTS: dict[str, str] = {
    "smartfoniy": "smartphones",
    "noutbuki": "laptops",
    "planshety": "tablets",
    "televizoriy": "tvs",
    "naushniki": "headphones",
    "igroviye_pristavki": "game_consoles",
    "fotoapparatiy": "cameras",
    "umnye_chasiy": "smart_watches",
    "monitory": "monitors",
    "kompyuternaya_periferiya": "peripherals",
    "holodilniki": "appliances",
    "kondicioneriy": "appliances",
    "led_oled_televizoriy": "tvs",
}

SATU_CATEGORIES: list[tuple[str, str]] = [
    ("Mobilnye-telefony", "smartphones"),
    ("Noutbuki", "laptops"),
    ("Planshety", "tablets"),
    ("Televizory", "tvs"),
    ("Kompyuternaya-tehnika-i-programmnoe-obespechenie", "computers"),
    ("Audio-i-aksessuary", "audio_accessories"),
]

@dataclass
class CleaningReport:
    steps: list[str] = field(default_factory=list)

    def add(self, msg: str) -> None:
        self.steps.append(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _sleep(a: float = 0.15, b: float = 0.35) -> None:
    time.sleep(random.uniform(a, b))


def fix_typos(text: str) -> str:
    """Лёгкая нормализация текста (пробелы, частые опечатки/регистр брендов)."""
    if not isinstance(text, str) or not text.strip():
        return text
    t = text.strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\biphone\b", "iPhone", t, flags=re.I)
    t = re.sub(r"\bsamsung\b", "Samsung", t, flags=re.I)
    t = re.sub(r"\bxiaomi\b", "Xiaomi", t, flags=re.I)
    return t


def sulpak_filter_urls(session: requests.Session, report: CleaningReport) -> list[str]:
    r = session.get(SULPAK_SITEMAP, timeout=60)
    r.raise_for_status()
    urls = [u for u in re.findall(r"<loc>([^<]+)</loc>", r.text) if "/f/" in u]
    report.add(f"Sulpak: из sitemap получено {len(urls)} URL фильтров/категорий.")
    random.seed(42)
    random.shuffle(urls)
    return urls


def sulpak_guess_category(product_path: str) -> str:
    m = re.search(r"/g/([^/?#]+)", product_path)
    if not m:
        return "other"
    first = m.group(1).split("-")[0].lower()
    return SULPAK_CATEGORY_HINTS.get(first, "other")


def parse_sulpak_product(html: str, product_url: str) -> dict[str, Any] | None:
    prices = re.findall(r'data-price="([0-9]+(?:\.[0-9]+)?)"', html)
    if not prices:
        return None
    price_vals = [float(p) for p in prices]
    price = float(np.median(price_vals))

    mt = re.search(r"<title>([^<]+)</title>", html, re.I)
    raw_title = mt.group(1) if mt else ""
    name = raw_title.split("—")[0].split(" - ")[0].strip()
    name = fix_typos(name)

    old_prices = re.findall(r'data-old-price="([0-9]+(?:\.[0-9]+)?)"', html)
    old_price = float(old_prices[0]) if old_prices else np.nan

    brand = np.nan
    bm = re.search(
        r'"brand"\s*:\s*\{\s*"@type"\s*:\s*"Brand"\s*,\s*"name"\s*:\s*"([^"]+)"',
        html,
    )
    if bm:
        brand = bm.group(1).strip()
    else:
        m2 = re.search(r"brandName[\"']?\s*:\s*[\"']([^\"']+)[\"']", html, re.I)
        if m2:
            brand = m2.group(1).strip()

    pid = product_url.rstrip("/").split("/")[-1]
    return {
        "source": "sulpak.kz",
        "product_id": f"SUL-{pid[:80]}",
        "product_name": name or pid,
        "brand": brand,
        "category": sulpak_guess_category(product_url),
        "price_kzt": price,
        "old_price_kzt": old_price,
        "currency": "KZT",
        "rating": np.nan,
        "reviews_count": np.nan,
        "seller_name": "Sulpak (retail)",
        "condition": "new",
        "product_url": product_url,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def collect_sulpak(
    session: requests.Session,
    target_slugs: int,
    report: CleaningReport,
) -> list[dict[str, Any]]:
    filters = sulpak_filter_urls(session, report)
    # Подвыборка страниц фильтров + параллельный обход (ускорение)
    max_filter_pages = min(200, len(filters))
    filters = filters[:max_filter_pages]
    report.add(f"Sulpak: для сбора slug обходим до {max_filter_pages} страниц фильтров (параллельно).")

    def _slugs_from_filter(fu: str) -> set[str]:
        try:
            html = requests.get(fu, headers=HEADERS, timeout=22).text
            return {h.split("?")[0] for h in re.findall(r'href="(/g/[^"?#]+)', html)}
        except requests.RequestException:
            return set()

    with ThreadPoolExecutor(max_workers=20) as pool:
        parts = list(pool.map(_slugs_from_filter, filters, chunksize=10))
    slugs: set[str] = set()
    for p in parts:
        slugs |= p

    slugs_list = list(slugs)[: max(target_slugs * 2, target_slugs + 50)]
    report.add(f"Sulpak: уникальных slug карточек для обхода: {len(slugs_list)}.")

    def _fetch_sulpak_card(path: str) -> dict[str, Any] | None:
        url = "https://www.sulpak.kz" + path
        try:
            html = requests.get(url, headers=HEADERS, timeout=25).text
            return parse_sulpak_product(html, url)
        except requests.RequestException:
            return None

    take = min(len(slugs_list), target_slugs * 2)
    with ThreadPoolExecutor(max_workers=16) as pool:
        raw = list(pool.map(_fetch_sulpak_card, slugs_list[:take], chunksize=32))
    rows = [r for r in raw if r][:target_slugs]

    report.add(f"Sulpak: успешно распарсено карточек: {len(rows)}.")
    return rows


def parse_satu_jsonld(html: str, url: str, category: str) -> dict[str, Any] | None:
    blocks = re.findall(
        r'<script type="application/ld\+json">(.*?)</script>', html, flags=re.DOTALL
    )
    product: dict[str, Any] | None = None
    for raw in blocks:
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "Product":
            product = data
            break
    if not product:
        return None

    offers = product.get("offers") or {}
    if isinstance(offers, list) and offers:
        offers = offers[0]
    price_s = offers.get("price") if isinstance(offers, dict) else None
    currency = offers.get("priceCurrency") if isinstance(offers, dict) else "KZT"
    try:
        price = float(str(price_s).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None

    seller = offers.get("seller") or {}
    seller_name = (
        seller.get("name") if isinstance(seller, dict) else str(seller or "unknown")
    )

    cond = offers.get("itemCondition", "")
    if isinstance(cond, str) and "Used" in cond:
        condition = "used"
    elif isinstance(cond, str) and "New" in cond:
        condition = "new"
    else:
        condition = "unknown"

    brand = product.get("brand") or {}
    if isinstance(brand, dict):
        bname = brand.get("name", np.nan)
    else:
        bname = str(brand) if brand else np.nan

    sku = str(product.get("sku") or url.rstrip("/").split("/")[-1].replace(".html", ""))
    name = fix_typos(str(product.get("name") or sku))

    return {
        "source": "satu.kz",
        "product_id": f"SAT-{sku[:60]}",
        "product_name": name,
        "brand": bname,
        "category": category,
        "price_kzt": price,
        "old_price_kzt": np.nan,
        "currency": currency or "KZT",
        "rating": np.nan,
        "reviews_count": np.nan,
        "seller_name": str(seller_name)[:200],
        "condition": condition,
        "product_url": url,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def collect_satu(
    session: requests.Session,
    target_rows: int,
    report: CleaningReport,
    max_page_per_category: int = 250,
) -> list[dict[str, Any]]:
    collected: list[tuple[str, str]] = []
    seen: set[str] = set()

    url_budget = int(target_rows * 1.25) + 200
    for path, cat in SATU_CATEGORIES:
        if len(collected) >= url_budget:
            break
        page = 1
        stagnant = 0
        while page <= max_page_per_category and len(collected) < url_budget:
            list_url = f"https://satu.kz/kz/{path}?page={page}"
            try:
                html = session.get(list_url, timeout=25).text
            except requests.RequestException as e:
                report.add(f"Satu: список {list_url}: {e}")
                break
            links = re.findall(r'href="(/kz/p[0-9]+[^"]+\.html)"', html)
            new = 0
            for href in links:
                full = "https://satu.kz" + href.split("?")[0]
                if full not in seen:
                    seen.add(full)
                    collected.append((full, cat))
                    new += 1
            if new == 0:
                stagnant += 1
                if stagnant >= 3:
                    break
            else:
                stagnant = 0
            page += 1
            _sleep(0.12, 0.3)

    report.add(f"Satu: собрано уникальных URL карточек: {len(collected)}.")

    work = collected[: target_rows * 2]

    def _fetch_satu_card(item: tuple[str, str]) -> dict[str, Any] | None:
        url, cat = item
        try:
            html = requests.get(url, headers=HEADERS, timeout=25).text
            return parse_satu_jsonld(html, url, cat)
        except requests.RequestException:
            return None

    with ThreadPoolExecutor(max_workers=16) as pool:
        raw = list(pool.map(_fetch_satu_card, work, chunksize=32))
    rows = [r for r in raw if r][:target_rows]

    report.add(f"Satu: успешно распарсено карточек: {len(rows)}.")
    return rows


def merge_and_clean(
    df: pd.DataFrame, report: CleaningReport
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Возвращает (очищенный, удалённые дубликаты, выбросы по IQR)."""
    n0 = len(df)
    report.add(f"Объединение: исходно {n0} строк.")

    df = df.copy()
    df["product_name"] = df["product_name"].apply(
        lambda x: fix_typos(str(x)) if pd.notna(x) else x
    )
    df["brand"] = df["brand"].apply(
        lambda x: str(x).strip().title()
        if pd.notna(x) and str(x).strip() and str(x) != "nan"
        else np.nan
    )

    # Неполные / устаревшие: нет цены или подозрительное название
    bad_name = df["product_name"].str.len() < 3
    report.add(f"Удалено строк с некорректным названием (<3 симв.): {int(bad_name.sum())}.")
    df = df[~bad_name]

    before = len(df)
    df = df[df["price_kzt"].notna() & (df["price_kzt"] > 0)]
    report.add(f"Удалено строк без положительной цены: {before - len(df)}.")

    # Дубликаты только по URL (одна карточка = один URL; разные объявления с одним названием — разные строки)
    dup_url = df.duplicated(subset=["product_url"], keep="first")
    report.add(f"Дубликаты по URL: {int(dup_url.sum())}.")
    df_dup = df[dup_url].copy()
    df = df[~dup_url]

    # Выбросы 1.5×IQR по цене
    q1 = df["price_kzt"].quantile(0.25)
    q3 = df["price_kzt"].quantile(0.75)
    iqr = q3 - q1
    low = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr
    out = (df["price_kzt"] < low) | (df["price_kzt"] > high)
    report.add(
        f"IQR: Q1={q1:,.0f}, Q3={q3:,.0f}, границы [{low:,.0f} – {high:,.0f}] KZT; выбросов: {int(out.sum())}."
    )
    df_out = df[out].copy()
    df_ok = df.copy()
    df_ok["price_outlier_iqr"] = out
    # «Исправление» выбросов: подрезка к границам коробки (данные сохраняем, строки не теряем)
    below = out & (df_ok["price_kzt"] < low)
    above = out & (df_ok["price_kzt"] > high)
    df_ok.loc[below, "price_kzt"] = low
    df_ok.loc[above, "price_kzt"] = high
    report.add(
        f"Выбросы скорректированы к границам IQR: ниже порога {int(below.sum())}, выше {int(above.sum())}."
    )

    def segment(p: float) -> str:
        if p < 50_000:
            return "low-priced"
        if p < 200_000:
            return "middle-priced"
        if p < 500_000:
            return "high-priced"
        return "luxury"

    df_ok["price_segment"] = df_ok["price_kzt"].apply(segment)

    report.add(f"Строк после корректировки цен по IQR: {len(df_ok)} (без удаления строк).")
    return df_ok, df_dup, df_out


def save_xml(df: pd.DataFrame, path: str) -> None:
    root = ET.Element("products")
    for _, row in df.iterrows():
        p = ET.SubElement(root, "product")
        for col in df.columns:
            child = ET.SubElement(p, col)
            val = row[col]
            if pd.isna(val):
                child.text = ""
            else:
                child.text = str(val)
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def run(quick: bool, raw_total: int) -> None:
    report = CleaningReport()
    if quick:
        n_sulpak = 60
        n_satu = 60
    else:
        # raw_total — примерное число сырых строк (два источника поровну)
        half = max(1700, (raw_total + 200) // 2)
        n_sulpak = half
        n_satu = half

    session = _session()
    report.add(
        f"Старт: quick={quick}, raw_total={raw_total}, карточек с каждого источника ≈ {n_sulpak}."
    )

    rows = collect_sulpak(session, n_sulpak, report)
    _sleep(0.5, 1.0)
    rows.extend(collect_satu(session, n_satu, report))

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("Нет данных — проверьте сеть и доступность сайтов.")

    df_clean, df_dup, df_out = merge_and_clean(df, report)

    out_csv = "qazaqprice_dataset.csv"
    df_clean.to_csv(out_csv, index=False, encoding="utf-8-sig")
    df_clean.to_json(
        "qazaqprice_dataset.json", orient="records", force_ascii=False, indent=2
    )
    save_xml(df_clean, "qazaqprice_dataset.xml")

    with open("data_cleaning_log.txt", "w", encoding="utf-8") as f:
        f.write("QazaqPrice — журнал очистки и объединения\n")
        f.write("Источники: https://www.sulpak.kz (sitemap + карточки), https://satu.kz (категории + JSON-LD)\n\n")
        for line in report.steps:
            f.write(line + "\n")
        f.write("\n--- Итог ---\n")
        f.write(f"Строк в финальном CSV: {len(df_clean)}\n")
        f.write(f"Дубликатов отфильтровано: {len(df_dup)}\n")
        f.write(f"Выбросов по IQR: {len(df_out)}\n\n")
        f.write("По источникам:\n")
        f.write(df_clean["source"].value_counts().to_string())
        f.write("\n\nПо ценовым сегментам:\n")
        f.write(df_clean["price_segment"].value_counts().to_string())
        f.write("\n\nОписательная статистика цены (KZT):\n")
        f.write(df_clean["price_kzt"].describe().to_string())

    print(f"Готово: {len(df_clean)} строк → {out_csv}")
    print(df_clean["source"].value_counts())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--raw-total",
        type=int,
        default=3200,
        help="Целевое число сырых строк до очистки (сумма двух источников; ≥3200 чтобы после дедупа осталось ≥2500)",
    )
    ap.add_argument("--quick", action="store_true", help="Быстрый прогон ~120 строк")
    args = ap.parse_args()
    run(quick=args.quick, raw_total=args.raw_total)


if __name__ == "__main__":
    main()
