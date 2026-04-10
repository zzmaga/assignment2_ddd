"""
QazaqPrice — Assignment №2: два открытых источника, ≥2500 строк после очистки.

Источник 1 (CSV, электроника / гаджеты):
  https://raw.githubusercontent.com/ArfaNada/Intelligent-Report-Generator/main/merged_electronics_dataset.csv
  Цены в индийских рупиях (₹) — конвертация в тенге через курс к USD (open.er-api.com).

Источник 2 (REST API, демо-каталог товаров):
  https://api.escuelajs.co/api/v1/products (пагинация offset/limit)
  Цены в USD — конвертация в ₸ через тот же API курсов.

"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import requests

GITHUB_ELECTRONICS_CSV = (
    "https://raw.githubusercontent.com/ArfaNada/Intelligent-Report-Generator/main/"
    "merged_electronics_dataset.csv"
)
ESCUELA_PRODUCTS = "https://api.escuelajs.co/api/v1/products"
FX_USD_URL = "https://open.er-api.com/v6/latest/USD"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; QazaqPrice/2; +education)",
    "Accept": "application/json, text/csv, */*",
}


@dataclass
class CleaningReport:
    steps: list[str] = field(default_factory=list)

    def add(self, msg: str) -> None:
        self.steps.append(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def fetch_fx_kzt_inr_per_usd() -> tuple[float, float]:
    """Возвращает (KZT за 1 USD, INR за 1 USD) из open.er-api.com."""
    r = requests.get(FX_USD_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()
    rates = data.get("rates") or {}
    kzt = float(rates["KZT"])
    inr = float(rates["INR"])
    return kzt, inr


def inr_to_kzt(amount: float, kzt_per_usd: float, inr_per_usd: float) -> float:
    return amount * (kzt_per_usd / inr_per_usd)


def usd_to_kzt(amount: float, kzt_per_usd: float) -> float:
    return amount * kzt_per_usd


def parse_inr_price(s: Any) -> float | None:
    if pd.isna(s):
        return None
    t = re.sub(r"[^\d.]", "", str(s).replace(",", ""))
    if not t:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def parse_int_count(s: Any) -> float:
    if pd.isna(s):
        return np.nan
    t = re.sub(r"[^\d]", "", str(s))
    if not t:
        return np.nan
    return float(t)


def load_github_electronics(
    kzt_per_usd: float,
    inr_per_usd: float,
    quick: bool,
    report: CleaningReport,
) -> pd.DataFrame:
    report.add(f"Загрузка CSV: {GITHUB_ELECTRONICS_CSV}")
    r = requests.get(GITHUB_ELECTRONICS_CSV, headers=HEADERS, timeout=120)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content))
    if quick:
        df = df.head(400).copy()

    rows: list[dict[str, Any]] = []
    for i, row in df.iterrows():
        disc = parse_inr_price(row.get("discount_price"))
        actual = parse_inr_price(row.get("actual_price"))
        price_inr = disc if disc is not None else actual
        if price_inr is None or price_inr <= 0:
            continue
        old_inr = actual if disc is not None and actual is not None else np.nan

        price_kzt = inr_to_kzt(price_inr, kzt_per_usd, inr_per_usd)
        old_kzt = (
            inr_to_kzt(old_inr, kzt_per_usd, inr_per_usd)
            if old_inr is not None and not np.isnan(old_inr) and disc is not None
            else np.nan
        )

        link = str(row.get("link") or "").strip()
        h = hashlib.sha256(link.encode("utf-8", errors="ignore")).hexdigest()[:16]
        main_cat = str(row.get("main_category") or "").strip()
        sub_cat = str(row.get("sub_category") or "").strip()
        cat = f"{main_cat} / {sub_cat}".strip(" /")

        rows.append(
            {
                "source": "github_amazon_in_electronics_csv",
                "product_id": f"CSV-{h}",
                "product_name": str(row.get("name") or "")[:500],
                "brand": np.nan,
                "category": cat or "electronics",
                "price_kzt": round(price_kzt, 2),
                "old_price_kzt": (
                    round(old_kzt, 2)
                    if old_kzt is not None
                    and not (isinstance(old_kzt, float) and np.isnan(old_kzt))
                    else np.nan
                ),
                "currency": "KZT",
                "price_original": price_inr,
                "currency_original": "INR",
                "rating": pd.to_numeric(row.get("review_rating"), errors="coerce"),
                "reviews_count": parse_int_count(row.get("no_of_ratings")),
                "seller_name": "Amazon India (open dataset)",
                "condition": "new",
                "product_url": link,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    report.add(
        f"CSV: строк после разбора цен: {len(rows)} (курс 1 USD = {kzt_per_usd:.2f} KZT, "
        f"1 USD = {inr_per_usd:.2f} INR → 1 INR = {kzt_per_usd/inr_per_usd:.4f} KZT)."
    )
    return pd.DataFrame(rows)


def load_escuelajs(
    kzt_per_usd: float,
    quick: bool,
    report: CleaningReport,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    offset = 0
    limit = 100
    max_rows = 120 if quick else 10_000

    while len(rows) < max_rows:
        url = f"{ESCUELA_PRODUCTS}?offset={offset}&limit={limit}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            report.add(f"Escuela: HTTP {r.status_code} на offset={offset}")
            break
        try:
            batch = r.json()
        except json.JSONDecodeError:
            break
        if not isinstance(batch, list) or not batch:
            break
        for p in batch:
            pid = p.get("id")
            title = str(p.get("title") or "")
            price_usd = p.get("price")
            try:
                pu = float(price_usd)
            except (TypeError, ValueError):
                continue
            if pu <= 0:
                continue
            cat = p.get("category") or {}
            cname = (
                str(cat.get("name") or cat.get("slug") or "general")
                if isinstance(cat, dict)
                else "general"
            )
            api_url = f"{ESCUELA_PRODUCTS}/{pid}"
            rows.append(
                {
                    "source": "escuelajs_demo_api",
                    "product_id": f"ESC-{pid}",
                    "product_name": title[:500],
                    "brand": np.nan,
                    "category": cname[:200],
                    "price_kzt": round(usd_to_kzt(pu, kzt_per_usd), 2),
                    "old_price_kzt": np.nan,
                    "currency": "KZT",
                    "price_original": pu,
                    "currency_original": "USD",
                    "rating": np.nan,
                    "reviews_count": np.nan,
                    "seller_name": "Escuela JS demo API",
                    "condition": "new",
                    "product_url": api_url,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            if quick and len(rows) >= 150:
                break
        if quick and len(rows) >= 150:
            break
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.05)

    report.add(
        f"Escuela JS: получено товаров: {len(rows)} (USD → KZT по {kzt_per_usd:.2f} ₸/$)."
    )
    return pd.DataFrame(rows)


def merge_and_clean(
    df: pd.DataFrame, report: CleaningReport
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    n0 = len(df)
    report.add(f"Объединение: исходно {n0} строк.")

    df = df.copy()
    df = df[df["product_name"].notna() & (df["product_name"].str.len() >= 3)]
    df = df[df["price_kzt"].notna() & (df["price_kzt"] > 0)]

    dup = df.duplicated(subset=["product_url"], keep="first")
    report.add(f"Дубликаты по URL: {int(dup.sum())}.")
    df_dup = df[dup].copy()
    df = df[~dup]

    q1 = df["price_kzt"].quantile(0.25)
    q3 = df["price_kzt"].quantile(0.75)
    iqr = q3 - q1
    low = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr
    out = (df["price_kzt"] < low) | (df["price_kzt"] > high)
    df_out = df[out].copy()
    df_ok = df.copy()
    df_ok["price_outlier_iqr"] = out
    df_ok.loc[out & (df_ok["price_kzt"] < low), "price_kzt"] = low
    df_ok.loc[out & (df_ok["price_kzt"] > high), "price_kzt"] = high
    report.add(
        f"IQR: границы [{low:,.0f} – {high:,.0f}] KZT; скорректировано выбросов: {int(out.sum())}."
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
    report.add(f"Итоговых строк: {len(df_ok)}.")
    return df_ok, df_dup, df_out


# 1. Удалите функцию save_xml целиком (строки 190-202 в вашем исходнике)


def run(quick: bool) -> None:
    report = CleaningReport()
    kzt_usd, inr_usd = fetch_fx_kzt_inr_per_usd()
    report.add(
        f"Курсы (open.er-api.com, base USD): KZT/USD={kzt_usd:.4f}, INR/USD={inr_usd:.4f}"
    )

    df_csv = load_github_electronics(kzt_usd, inr_usd, quick, report)
    time.sleep(0.2)
    df_api = load_escuelajs(kzt_usd, quick, report)

    df = pd.concat([df_csv, df_api], ignore_index=True)
    if not quick and len(df) < 2500:
        print(f"Внимание: собрано всего {len(df)} строк. Нужно минимум 2500!")

    df_clean, df_dup, df_out = merge_and_clean(df, report)

    out_csv = "qazaqprice_dataset.csv"
    df_clean.to_csv(out_csv, index=False, encoding="utf-8-sig")

    with open("data_cleaning_log.txt", "w", encoding="utf-8") as f:
        f.write("QazaqPrice — журнал очистки\n\n")
        f.write("Источник 1: " + GITHUB_ELECTRONICS_CSV + "\n")
        f.write("Источник 2: " + ESCUELA_PRODUCTS + " (пагинация)\n")
        f.write("Курсы: " + FX_USD_URL + "\n\n")
        for line in report.steps:
            f.write(line + "\n")
        f.write("\n--- Итог ---\n")
        f.write(f"Строк в CSV: {len(df_clean)}\n")
        f.write(f"Дубликатов: {len(df_dup)}\n")
        f.write(f"Выбросов по IQR (до коррекции): {len(df_out)}\n")
        f.write("\nПо источникам:\n")
        f.write(df_clean["source"].value_counts().to_string())
    print(f"Готово: {len(df_clean)} строк → {out_csv}")
    print(df_clean["source"].value_counts())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()
    run(quick=args.quick)


if __name__ == "__main__":
    main()
