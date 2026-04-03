"""
Строит HTML-дашборд по qazaqprice_dataset.csv (Assignment №2, задание 2).
Требует: pandas, matplotlib
Запуск: python dashboard.py
"""

from __future__ import annotations

import base64
from io import BytesIO
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

CSV_PATH = Path("qazaqprice_dataset.csv")
OUT_HTML = Path("dashboard.html")


def fig_to_b64(fig) -> str:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("ascii")


def main() -> None:
    if not CSV_PATH.exists():
        raise SystemExit(f"Нет файла {CSV_PATH}. Сначала выполните: python pipeline.py")

    df = pd.read_csv(CSV_PATH)
    df["price_kzt"] = pd.to_numeric(df["price_kzt"], errors="coerce")

    charts: list[tuple[str, str]] = []

    # 1. Источники
    fig1, ax = plt.subplots(figsize=(7, 4))
    vc = df["source"].value_counts()
    ax.bar(vc.index.astype(str), vc.values, color=["#c62828", "#1565c0"])
    ax.set_title("Распределение объявлений по источникам-конкурентам")
    ax.set_ylabel("Количество позиций")
    charts.append(("Источники данных", fig_to_b64(fig1)))

    # 2. Ценовые сегменты
    fig2, ax = plt.subplots(figsize=(7, 4))
    seg_order = ["low-priced", "middle-priced", "high-priced", "luxury"]
    seg = df["price_segment"].value_counts()
    seg = seg.reindex([s for s in seg_order if s in seg.index])
    ax.bar(seg.index.astype(str), seg.values, color="#2e7d32")
    ax.set_title("Позиции по ценовому сегменту")
    ax.set_ylabel("Количество")
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=25, ha="right")
    charts.append(("Ценовые сегменты", fig_to_b64(fig2)))

    # 3. Категории (топ-12)
    fig3, ax = plt.subplots(figsize=(8, 5))
    cat = df["category"].value_counts().head(12)
    ax.barh(cat.index.astype(str)[::-1], cat.values[::-1], color="#6a1b9a")
    ax.set_title("Топ категорий по числу позиций")
    charts.append(("Категории", fig_to_b64(fig3)))

    # 4. Гистограмма цен (log)
    fig4, ax = plt.subplots(figsize=(7, 4))
    prices = df["price_kzt"].dropna()
    ax.hist(prices, bins=50, color="#ef6c00", edgecolor="white")
    ax.set_xlabel("Цена, ₸")
    ax.set_title("Распределение цен (KZT)")
    charts.append(("Распределение цен", fig_to_b64(fig4)))

    # 5. Satu: условие товара
    if "condition" in df.columns:
        fig5, ax = plt.subplots(figsize=(6, 4))
        sub = df[df["source"] == "satu.kz"]
        if len(sub):
            c = sub["condition"].fillna("unknown").value_counts()
            ax.pie(c.values, labels=c.index, autopct="%1.0f%%")
            ax.set_title("Satu.kz: новый / б/у (доля)")
        charts.append(("Satu: состояние товара", fig_to_b64(fig5)))

    parts = [
        "<!DOCTYPE html><html lang='ru'><head><meta charset='utf-8'/>",
        "<title>QazaqPrice — дашборд</title>",
        "<style>body{font-family:Segoe UI,Roboto,sans-serif;max-width:960px;margin:24px auto;background:#fafafa;color:#222;}",
        "h1{color:#c62828;} section{margin-bottom:40px;} img{max-width:100%;border:1px solid #ddd;border-radius:8px;background:#fff;}</style></head><body>",
        "<h1>QazaqPrice — рыночная разведка (дашборд)</h1>",
        "<p>Автоматически сгенерировано из <code>qazaqprice_dataset.csv</code>.</p>",
    ]
    for title, b64 in charts:
        parts.append(f"<section><h2>{title}</h2>")
        parts.append(f"<img src='data:image/png;base64,{b64}' alt='{title}'/>")
        parts.append("</section>")
    parts.append("</body></html>")

    OUT_HTML.write_text("".join(parts), encoding="utf-8")
    print(f"Сохранено: {OUT_HTML.resolve()}")


if __name__ == "__main__":
    main()
