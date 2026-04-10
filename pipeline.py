"""
QazaqPrice - Assignment #2: two open sources, >=2500 rows after cleaning.

Source 1:
  https://raw.githubusercontent.com/ArfaNada/Intelligent-Report-Generator/main/merged_electronics_dataset.csv

Source 2:
  https://api.escuelajs.co/api/v1/products

Source 3:
  live product pages from official Apple, Samsung, and Google stores
  for modern gadgets such as iPhone 16, Galaxy S25, and Pixel 9.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import io
import re
import time
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
    "User-Agent": "Mozilla/5.0 (compatible; QazaqPrice/3; +education)",
    "Accept": "text/html,application/json,text/csv,*/*",
}


@dataclass
class CleaningReport:
    steps: list[str] = field(default_factory=list)

    def add(self, msg: str) -> None:
        self.steps.append(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


@dataclass(frozen=True)
class LiveProductSeed:
    source: str
    brand: str
    product_name: str
    category: str
    url: str
    price_patterns: tuple[str, ...]
    currency_original: str = "USD"


LIVE_PRODUCT_SEEDS: tuple[LiveProductSeed, ...] = (
    LiveProductSeed(
        source="apple_official_store_live",
        brand="Apple",
        product_name="Apple iPhone 16 128GB",
        category="smartphones",
        url="https://www.apple.com/shop/buy-iphone/iphone-16/6.1-inch-display-128gb-black-unlocked",
        price_patterns=(
            r"Buy iPhone\s*16[\s\S]{0,500}?\$(\d[\d,]*(?:\.\d{2})?)",
            r"iPhone\s*16[\s\S]{0,250}?\$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
    LiveProductSeed(
        source="apple_official_store_live",
        brand="Apple",
        product_name="Apple AirPods Pro 2",
        category="audio",
        url="https://www.apple.com/shop/buy-airpods/airpods-pro-2",
        price_patterns=(
            r"Buy AirPods Pro 2[\s\S]{0,250}?\$(\d[\d,]*(?:\.\d{2})?)",
            r"AirPods Pro 2[\s\S]{0,150}?\$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
    LiveProductSeed(
        source="apple_official_store_live",
        brand="Apple",
        product_name="Apple iPad Air",
        category="tablets",
        url="https://www.apple.com/shop/buy-ipad/ipad-air",
        price_patterns=(
            r"Buy iPad Air[\s\S]{0,350}?\$(\d[\d,]*(?:\.\d{2})?)",
            r"iPad Air[\s\S]{0,150}?From \$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
    LiveProductSeed(
        source="apple_official_store_live",
        brand="Apple",
        product_name="Apple MacBook Air",
        category="laptops",
        url="https://www.apple.com/shop/buy-mac/macbook-air",
        price_patterns=(
            r"MacBook Air[\s\S]{0,400}?\$(\d[\d,]*(?:\.\d{2})?)",
            r"Choose your new MacBook Air[\s\S]{0,500}?\$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
    LiveProductSeed(
        source="google_store_live",
        brand="Google",
        product_name="Google Pixel 9",
        category="smartphones",
        url="https://store.google.com/us/product/pixel_9?hl=en-US",
        price_patterns=(
            r"Pixel 9[\s\S]{0,180}?From \$(\d[\d,]*(?:\.\d{2})?)",
            r"Pixel 9[\s\S]{0,120}?\$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
    LiveProductSeed(
        source="google_store_live",
        brand="Google",
        product_name="Google Pixel 9 Pro",
        category="smartphones",
        url="https://store.google.com/us/product/pixel_9_pro?hl=en-US",
        price_patterns=(
            r"Pixel 9 Pro[\s\S]{0,180}?From \$(\d[\d,]*(?:\.\d{2})?)",
            r"Pixel 9 Pro[\s\S]{0,120}?\$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
    LiveProductSeed(
        source="google_store_live",
        brand="Google",
        product_name="Google Pixel Tablet",
        category="tablets",
        url="https://store.google.com/product/pixel_tablet?hl=en-US&pli=1",
        price_patterns=(
            r"Pixel Tablet[\s\S]{0,180}?From \$(\d[\d,]*(?:\.\d{2})?)",
            r"Pixel Tablet[\s\S]{0,120}?\$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
    LiveProductSeed(
        source="samsung_official_store_live",
        brand="Samsung",
        product_name="Samsung Galaxy S25",
        category="smartphones",
        url="https://www.samsung.com/us/smartphones/galaxy-s25/",
        price_patterns=(
            r"Galaxy S25[\s\S]{0,220}?From \$(\d[\d,]*(?:\.\d{2})?)",
            r"Galaxy S25[\s\S]{0,150}?\$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
    LiveProductSeed(
        source="samsung_official_store_live",
        brand="Samsung",
        product_name="Samsung Galaxy S25 Ultra",
        category="smartphones",
        url="https://www.samsung.com/us/smartphones/galaxy-s25-ultra/",
        price_patterns=(
            r"Galaxy S25 Ultra[\s\S]{0,220}?From \$(\d[\d,]*(?:\.\d{2})?)",
            r"Galaxy S25 Ultra[\s\S]{0,150}?\$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
    LiveProductSeed(
        source="samsung_official_store_live",
        brand="Samsung",
        product_name="Samsung Galaxy Tab S10",
        category="tablets",
        url="https://www.samsung.com/us/tablets/galaxy-tab-s10/",
        price_patterns=(
            r"Galaxy Tab S10[\s\S]{0,220}?From \$(\d[\d,]*(?:\.\d{2})?)",
            r"Galaxy Tab S10[\s\S]{0,150}?\$(\d[\d,]*(?:\.\d{2})?)",
        ),
    ),
)


def fetch_fx_kzt_inr_per_usd() -> tuple[float, float]:
    """Returns (KZT per USD, INR per USD) from open.er-api.com."""
    response = requests.get(FX_USD_URL, headers=HEADERS, timeout=20)
    response.raise_for_status()
    rates = response.json().get("rates") or {}
    return float(rates["KZT"]), float(rates["INR"])


def inr_to_kzt(amount: float, kzt_per_usd: float, inr_per_usd: float) -> float:
    return amount * (kzt_per_usd / inr_per_usd)


def usd_to_kzt(amount: float, kzt_per_usd: float) -> float:
    return amount * kzt_per_usd


def parse_inr_price(value: Any) -> float | None:
    if pd.isna(value):
        return None
    cleaned = re.sub(r"[^\d.]", "", str(value).replace(",", ""))
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int_count(value: Any) -> float:
    if pd.isna(value):
        return np.nan
    cleaned = re.sub(r"[^\d]", "", str(value))
    if not cleaned:
        return np.nan
    return float(cleaned)


def fetch_text(url: str, timeout: int = 30) -> str:
    response = requests.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def normalize_html_text(raw_html: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", raw_html)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def extract_price(text: str, patterns: tuple[str, ...]) -> float | None:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            try:
                return float(match.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


def build_product_id(prefix: str, key: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def load_github_electronics(
    kzt_per_usd: float,
    inr_per_usd: float,
    quick: bool,
    report: CleaningReport,
) -> pd.DataFrame:
    report.add(f"Loading CSV: {GITHUB_ELECTRONICS_CSV}")
    response = requests.get(GITHUB_ELECTRONICS_CSV, headers=HEADERS, timeout=120)
    response.raise_for_status()
    df = pd.read_csv(io.BytesIO(response.content))
    if quick:
        df = df.head(400).copy()

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
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
        main_cat = str(row.get("main_category") or "").strip()
        sub_cat = str(row.get("sub_category") or "").strip()
        category = f"{main_cat} / {sub_cat}".strip(" /")

        rows.append(
            {
                "source": "github_amazon_in_electronics_csv",
                "product_id": build_product_id(
                    "CSV", link or str(row.get("name") or "")
                ),
                "product_name": str(row.get("name") or "")[:500],
                "brand": np.nan,
                "category": category or "electronics",
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
        f"CSV rows after price parsing: {len(rows)} "
        f"(1 USD = {kzt_per_usd:.2f} KZT, 1 USD = {inr_per_usd:.2f} INR)."
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
    max_rows = 120 if quick else 2_000
    allowed_categories = {"electronics"}

    while len(rows) < max_rows:
        url = f"{ESCUELA_PRODUCTS}?offset={offset}&limit={limit}"
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            report.add(f"Escuela JS HTTP {response.status_code} at offset={offset}")
            break
        try:
            batch = response.json()
        except ValueError:
            report.add("Escuela JS returned invalid JSON.")
            break
        if not isinstance(batch, list) or not batch:
            break

        for product in batch:
            pid = product.get("id")
            title = str(product.get("title") or "")
            category_info = product.get("category") or {}
            category_name = (
                str(category_info.get("name") or category_info.get("slug") or "general")
                if isinstance(category_info, dict)
                else "general"
            )
            if category_name.lower() not in allowed_categories:
                continue

            try:
                price_usd = float(product.get("price"))
            except (TypeError, ValueError):
                continue
            if price_usd <= 0:
                continue

            rows.append(
                {
                    "source": "escuelajs_demo_api",
                    "product_id": f"ESC-{pid}",
                    "product_name": title[:500],
                    "brand": np.nan,
                    "category": category_name[:200],
                    "price_kzt": round(usd_to_kzt(price_usd, kzt_per_usd), 2),
                    "old_price_kzt": np.nan,
                    "currency": "KZT",
                    "price_original": price_usd,
                    "currency_original": "USD",
                    "rating": np.nan,
                    "reviews_count": np.nan,
                    "seller_name": "Escuela JS demo API",
                    "condition": "new",
                    "product_url": f"{ESCUELA_PRODUCTS}/{pid}",
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            if quick and len(rows) >= 60:
                break

        if quick and len(rows) >= 60:
            break
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.05)

    report.add(
        f"Escuela JS modern electronics rows: {len(rows)} "
        f"(USD to KZT using {kzt_per_usd:.2f} KZT/USD)."
    )
    return pd.DataFrame(rows)


def load_live_modern_catalog(
    kzt_per_usd: float,
    quick: bool,
    report: CleaningReport,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    seeds = LIVE_PRODUCT_SEEDS[:5] if quick else LIVE_PRODUCT_SEEDS

    for seed in seeds:
        try:
            text = normalize_html_text(fetch_text(seed.url, timeout=40))
            price_original = extract_price(text, seed.price_patterns)
        except requests.RequestException as exc:
            report.add(f"Live source failed for {seed.product_name}: {exc}")
            continue

        if price_original is None or price_original <= 0:
            report.add(f"Live source price not found for {seed.product_name}.")
            continue

        rows.append(
            {
                "source": seed.source,
                "product_id": build_product_id("LIVE", seed.url),
                "product_name": seed.product_name,
                "brand": seed.brand,
                "category": seed.category,
                "price_kzt": round(usd_to_kzt(price_original, kzt_per_usd), 2),
                "old_price_kzt": np.nan,
                "currency": "KZT",
                "price_original": price_original,
                "currency_original": seed.currency_original,
                "rating": np.nan,
                "reviews_count": np.nan,
                "seller_name": f"{seed.brand} official store",
                "condition": "new",
                "product_url": seed.url,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        time.sleep(0.15)

    report.add(f"Live modern gadget rows collected: {len(rows)}.")
    return pd.DataFrame(rows)


def merge_and_clean(
    df: pd.DataFrame, report: CleaningReport
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    report.add(f"Combined rows before cleaning: {len(df)}.")

    df = df.copy()
    df = df[df["product_name"].notna() & (df["product_name"].str.len() >= 3)]
    df = df[df["price_kzt"].notna() & (df["price_kzt"] > 0)]
    df["brand"] = df["brand"].fillna("")
    df["category"] = df["category"].fillna("electronics")
    df["product_url"] = df["product_url"].fillna("")

    dedupe_key = np.where(
        df["product_url"].str.strip().ne(""),
        df["product_url"].str.strip().str.lower(),
        (
            df["product_name"].str.strip().str.lower()
            + "|"
            + df["brand"].str.strip().str.lower()
            + "|"
            + df["price_kzt"].round(2).astype(str)
        ),
    )
    dup_mask = pd.Series(dedupe_key, index=df.index).duplicated(keep="first")
    df_dup = df[dup_mask].copy()
    df = df[~dup_mask].copy()
    report.add(f"Duplicates removed: {int(dup_mask.sum())}.")

    q1 = df["price_kzt"].quantile(0.25)
    q3 = df["price_kzt"].quantile(0.75)
    iqr = q3 - q1
    low = max(q1 - 1.5 * iqr, 0)
    high = q3 + 1.5 * iqr
    out_mask = (df["price_kzt"] < low) | (df["price_kzt"] > high)
    df_out = df[out_mask].copy()

    # Premium gadgets should stay premium, so we flag outliers but keep real prices.
    df["price_outlier_iqr"] = out_mask
    report.add(
        f"IQR range [{low:,.0f} - {high:,.0f}] KZT; flagged outliers without price clipping: "
        f"{int(out_mask.sum())}."
    )

    def segment(price_kzt: float) -> str:
        if price_kzt < 80_000:
            return "low-priced"
        if price_kzt < 250_000:
            return "middle-priced"
        if price_kzt < 600_000:
            return "high-priced"
        return "luxury"

    df["price_segment"] = df["price_kzt"].apply(segment)
    report.add(f"Rows after cleaning: {len(df)}.")
    return df, df_dup, df_out


def run(quick: bool) -> None:
    report = CleaningReport()
    kzt_usd, inr_usd = fetch_fx_kzt_inr_per_usd()
    report.add(
        f"FX rates from {FX_USD_URL}: KZT/USD={kzt_usd:.4f}, INR/USD={inr_usd:.4f}."
    )

    df_csv = load_github_electronics(kzt_usd, inr_usd, quick, report)
    time.sleep(0.2)
    df_api = load_escuelajs(kzt_usd, quick, report)
    time.sleep(0.2)
    df_live = load_live_modern_catalog(kzt_usd, quick, report)

    df_all = pd.concat([df_csv, df_api, df_live], ignore_index=True)
    if not quick and len(df_all) < 2500:
        print(
            f"Warning: only {len(df_all)} rows collected before cleaning. Need at least 2500."
        )

    df_clean, df_dup, df_out = merge_and_clean(df_all, report)

    out_csv = "qazaqprice_dataset.csv"
    df_clean.to_csv(out_csv, index=False, encoding="utf-8-sig")

    with open("data_cleaning_log.txt", "w", encoding="utf-8") as handle:
        handle.write("QazaqPrice - data cleaning log\n\n")
        handle.write("Source 1: " + GITHUB_ELECTRONICS_CSV + "\n")
        handle.write("Source 2: " + ESCUELA_PRODUCTS + " (electronics only)\n")
        handle.write("Source 3: Apple / Samsung / Google official store pages\n")
        handle.write("FX: " + FX_USD_URL + "\n\n")
        for line in report.steps:
            handle.write(line + "\n")
        handle.write("\n--- Summary ---\n")
        handle.write(f"Rows in CSV: {len(df_clean)}\n")
        handle.write(f"Duplicates: {len(df_dup)}\n")
        handle.write(f"IQR outliers flagged: {len(df_out)}\n")
        handle.write("\nBy source:\n")
        handle.write(df_clean["source"].value_counts().to_string())
        handle.write("\n\nBy price segment:\n")
        handle.write(df_clean["price_segment"].value_counts().to_string())

    print(f"Done: {len(df_clean)} rows -> {out_csv}")
    print(df_clean["source"].value_counts())
    print(df_clean["price_segment"].value_counts())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()
    run(quick=args.quick)


if __name__ == "__main__":
    main()
