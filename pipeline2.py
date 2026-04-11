"""
QazaqPrice - Assignment #2: three open sources, >=2500 rows after cleaning.

Source 1:
  https://raw.githubusercontent.com/ArfaNada/Intelligent-Report-Generator/main/merged_electronics_dataset.csv
  Amazon India electronics CSV. Prices in INR → converted to KZT.
  FIX: brand extracted from product_name via regex list.

Source 2:
  https://api.bestbuy.com/v1/products
  Best Buy open API (free tier, no key needed for basic queries).
  Modern gadgets with USD prices and real brand field.

Source 3:
  https://dummyjson.com/products/category/smartphones  (+ laptops, tablets)
  DummyJSON — free REST API, no auth, modern product names, USD prices, brand field included.
"""

from __future__ import annotations

import argparse
import hashlib
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
DUMMYJSON_BASE = "https://dummyjson.com/products"
FX_USD_URL = "https://open.er-api.com/v6/latest/USD"

DUMMYJSON_CATEGORIES = [
    "smartphones",
    "laptops",
    "tablets",
    "mobile-accessories",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; QazaqPrice/4; +education)",
    "Accept": "application/json,text/csv,*/*",
}

# ── Brand extraction patterns for CSV (Amazon India names) ──────────────────
# Format: (regex_pattern, canonical_brand_name)
BRAND_PATTERNS: list[tuple[str, str]] = [
    (r"\bApple\b", "Apple"),
    (r"\bSamsung\b", "Samsung"),
    (r"\bSony\b", "Sony"),
    (r"\bLG\b", "LG"),
    (r"\bPhilips\b", "Philips"),
    (r"\bBosch\b", "Bosch"),
    (r"\bPanasonic\b", "Panasonic"),
    (r"\bMicrosoft\b", "Microsoft"),
    (r"\bLenovo\b", "Lenovo"),
    (r"\bHP\b|\bHewlett[- ]Packard\b", "HP"),
    (r"\bDell\b", "Dell"),
    (r"\bASUS\b|\bAsus\b", "ASUS"),
    (r"\bAcer\b", "Acer"),
    (r"\bMSI\b", "MSI"),
    (r"\bHuawei\b", "Huawei"),
    (r"\bXiaomi\b|\bRedmi\b|\bPoco\b", "Xiaomi"),
    (r"\bOnePlus\b", "OnePlus"),
    (r"\bRealme\b", "Realme"),
    (r"\bOppo\b", "OPPO"),
    (r"\bVivo\b", "Vivo"),
    (r"\bNokia\b", "Nokia"),
    (r"\bMotorola\b|\bMoto[- ][A-Z]", "Motorola"),
    (r"\bIntel\b", "Intel"),
    (r"\bAMD\b", "AMD"),
    (r"\bNVIDIA\b|\bGeForce\b", "NVIDIA"),
    (r"\bWD\b|\bWestern Digital\b", "Western Digital"),
    (r"\bSeagate\b", "Seagate"),
    (r"\bSandisk\b|\bSanDisk\b", "SanDisk"),
    (r"\bKingston\b", "Kingston"),
    (r"\bCorsair\b", "Corsair"),
    (r"\bLogitech\b", "Logitech"),
    (r"\bBose\b", "Bose"),
    (r"\bJBL\b", "JBL"),
    (r"\bSennheiser\b", "Sennheiser"),
    (r"\bAKG\b", "AKG"),
    (r"\bBoAt\b|\bboAt\b", "boAt"),
    (r"\bptron\b|\bpTron\b", "PTron"),
    (r"\bAnker\b", "Anker"),
    (r"\bUGreen\b|\bUGREEN\b", "UGREEN"),
    (r"\bBaseus\b", "Baseus"),
    (r"\bBelkin\b", "Belkin"),
    (r"\bGoogle\b|\bPixel\b", "Google"),
    (r"\bOnkyo\b", "Onkyo"),
    (r"\bDenon\b", "Denon"),
    (r"\bYamaha\b", "Yamaha"),
    (r"\bAmazon\b|\bEcho\b|\bKindle\b|\bFire\b", "Amazon"),
    (r"\bTP[- ]?Link\b", "TP-Link"),
    (r"\bNetgear\b", "Netgear"),
    (r"\bD-Link\b|\bDlink\b", "D-Link"),
    (r"\biball\b|\biBall\b", "iBall"),
    (r"\bIntex\b", "Intex"),
    (r"\bMicromax\b", "Micromax"),
    (r"\bLava\b", "Lava"),
    (r"\bJio\b", "Jio"),
]

_COMPILED_BRANDS = [(re.compile(p, re.IGNORECASE), b) for p, b in BRAND_PATTERNS]


def extract_brand(product_name: str) -> str:
    """Return brand name found in product_name, or empty string."""
    for pattern, brand in _COMPILED_BRANDS:
        if pattern.search(product_name):
            return brand
    return ""


@dataclass
class CleaningReport:
    steps: list[str] = field(default_factory=list)

    def add(self, msg: str) -> None:
        self.steps.append(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


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


def build_product_id(prefix: str, key: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{prefix}-{digest}"


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
    # Guard against lists/dicts returned by APIs (e.g. DummyJSON "reviews" is a list)
    if isinstance(value, (list, dict)):
        return float(len(value)) if isinstance(value, list) else np.nan
    try:
        if pd.isna(value):
            return np.nan
    except (TypeError, ValueError):
        return np.nan
    cleaned = re.sub(r"[^\d]", "", str(value))
    return float(cleaned) if cleaned else np.nan


# ── Source 1: GitHub Electronics CSV (Amazon India) ──────────────────────────


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
            if old_inr is not None
            and not (isinstance(old_inr, float) and np.isnan(old_inr))
            and disc is not None
            else np.nan
        )

        name = str(row.get("name") or "")
        link = str(row.get("link") or "").strip()
        main_cat = str(row.get("main_category") or "").strip()
        sub_cat = str(row.get("sub_category") or "").strip()
        category = f"{main_cat} / {sub_cat}".strip(" /")

        # FIX: extract brand from product name instead of leaving blank
        brand = extract_brand(name)

        rows.append(
            {
                "source": "github_amazon_in_electronics_csv",
                "product_id": build_product_id("CSV", link or name),
                "product_name": name[:500],
                "brand": brand,
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


# ── Source 2: DummyJSON products API ─────────────────────────────────────────
# Free, no auth, returns brand, category, price (USD), rating.
# Modern product names: iPhone 14, Galaxy S21, MacBook Pro, etc.


def load_dummyjson(
    kzt_per_usd: float,
    quick: bool,
    report: CleaningReport,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    categories = DUMMYJSON_CATEGORIES[:2] if quick else DUMMYJSON_CATEGORIES

    for cat in categories:
        url = f"{DUMMYJSON_BASE}/category/{cat}?limit=100"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as exc:
            report.add(f"DummyJSON category '{cat}' failed: {exc}")
            continue

        products = data.get("products") or []
        for p in products:
            price_usd = None
            try:
                price_usd = float(p.get("price") or 0)
            except (TypeError, ValueError):
                pass
            if not price_usd or price_usd <= 0:
                continue

            brand = str(p.get("brand") or "").strip()
            if not brand:
                brand = extract_brand(str(p.get("title") or ""))

            rows.append(
                {
                    "source": "dummyjson_api",
                    "product_id": f"DJS-{p.get('id')}",
                    "product_name": str(p.get("title") or "")[:500],
                    "brand": brand,
                    "category": cat,
                    "price_kzt": round(usd_to_kzt(price_usd, kzt_per_usd), 2),
                    "old_price_kzt": np.nan,
                    "currency": "KZT",
                    "price_original": price_usd,
                    "currency_original": "USD",
                    "rating": pd.to_numeric(p.get("rating"), errors="coerce"),
                    "reviews_count": parse_int_count(p.get("reviews", np.nan)),
                    "seller_name": "DummyJSON API",
                    "condition": "new",
                    "product_url": f"{DUMMYJSON_BASE}/{p.get('id')}",
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        time.sleep(0.1)

    report.add(f"DummyJSON rows: {len(rows)}.")
    return pd.DataFrame(rows)


# ── Source 3: FakeStoreAPI ────────────────────────────────────────────────────
# Another free API: https://fakestoreapi.com/products
# Returns title, price (USD), category, rating.count, rating.rate
# Good supplement for electronics/clothing mix — we filter electronics only.

FAKESTOREAPI = "https://fakestoreapi.com/products"


def load_fakestoreapi(
    kzt_per_usd: float,
    quick: bool,
    report: CleaningReport,
) -> pd.DataFrame:
    try:
        resp = requests.get(FAKESTOREAPI, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        products = resp.json()
    except (requests.RequestException, ValueError) as exc:
        report.add(f"FakeStoreAPI failed: {exc}")
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for p in products:
        cat = str(p.get("category") or "").lower()
        if "electronic" not in cat:
            continue
        try:
            price_usd = float(p.get("price") or 0)
        except (TypeError, ValueError):
            continue
        if price_usd <= 0:
            continue

        title = str(p.get("title") or "")
        rating_obj = p.get("rating") or {}
        rows.append(
            {
                "source": "fakestoreapi",
                "product_id": f"FSA-{p.get('id')}",
                "product_name": title[:500],
                "brand": extract_brand(title),
                "category": "electronics",
                "price_kzt": round(usd_to_kzt(price_usd, kzt_per_usd), 2),
                "old_price_kzt": np.nan,
                "currency": "KZT",
                "price_original": price_usd,
                "currency_original": "USD",
                "rating": pd.to_numeric(rating_obj.get("rate"), errors="coerce"),
                "reviews_count": parse_int_count(rating_obj.get("count")),
                "seller_name": "FakeStoreAPI",
                "condition": "new",
                "product_url": f"{FAKESTOREAPI}/{p.get('id')}",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    report.add(f"FakeStoreAPI electronics rows: {len(rows)}.")
    return pd.DataFrame(rows)


# ── Cleaning ──────────────────────────────────────────────────────────────────


def merge_and_clean(
    df: pd.DataFrame, report: CleaningReport
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    report.add(f"Combined rows before cleaning: {len(df)}.")

    df = df.copy()
    df = df[df["product_name"].notna() & (df["product_name"].str.len() >= 3)]
    df = df[df["price_kzt"].notna() & (df["price_kzt"] > 0)]
    df["brand"] = df["brand"].fillna("").str.strip()
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

    df["price_outlier_iqr"] = out_mask
    report.add(
        f"IQR range [{low:,.0f} – {high:,.0f}] KZT; flagged outliers (not clipped): "
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


# ── Main ──────────────────────────────────────────────────────────────────────


def run(quick: bool) -> None:
    report = CleaningReport()
    kzt_usd, inr_usd = fetch_fx_kzt_inr_per_usd()
    report.add(
        f"FX rates from {FX_USD_URL}: KZT/USD={kzt_usd:.4f}, INR/USD={inr_usd:.4f}."
    )

    df_csv = load_github_electronics(kzt_usd, inr_usd, quick, report)
    time.sleep(0.2)
    df_dummy = load_dummyjson(kzt_usd, quick, report)
    time.sleep(0.2)
    df_fake = load_fakestoreapi(kzt_usd, quick, report)

    df_all = pd.concat([df_csv, df_dummy, df_fake], ignore_index=True)
    if not quick and len(df_all) < 2500:
        print(
            f"Warning: only {len(df_all)} rows collected before cleaning. "
            "Need at least 2500."
        )

    df_clean, df_dup, df_out = merge_and_clean(df_all, report)

    out_csv = "qazaqprice_dataset.csv"
    df_clean.to_csv(out_csv, index=False, encoding="utf-8-sig")

    with open("data_cleaning_log.txt", "w", encoding="utf-8") as handle:
        handle.write("QazaqPrice – data cleaning log\n\n")
        handle.write("Source 1: " + GITHUB_ELECTRONICS_CSV + "\n")
        handle.write(
            "Source 2: "
            + DUMMYJSON_BASE
            + " (smartphones, laptops, tablets, accessories)\n"
        )
        handle.write("Source 3: " + FAKESTOREAPI + " (electronics category)\n")
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
        handle.write("\n\nTop brands:\n")
        handle.write(
            df_clean["brand"].replace("", "brand").value_counts().head(15).to_string()
        )

    print(f"Done: {len(df_clean)} rows -> {out_csv}")
    print("\n--- By source ---")
    print(df_clean["source"].value_counts())
    print("\n--- By segment ---")
    print(df_clean["price_segment"].value_counts())
    print("\n--- Top brands ---")
    print(df_clean["brand"].replace("", "brand").value_counts().head(10))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--quick", action="store_true", help="Fast test run (~200 rows)"
    )
    args = parser.parse_args()
    run(quick=args.quick)


if __name__ == "__main__":
    main()
