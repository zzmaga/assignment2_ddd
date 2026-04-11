"""Analysis and visualization for QazaqPrice dataset."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

SOURCE_CSV = Path("qazaqprice_dataset.csv")
OUTPUT_DIR = Path("plots")

SEGMENT_ORDER = ["low-priced", "middle-priced", "high-priced", "luxury"]
SEGMENT_COLORS = ["#28a745", "#007bff", "#ffc107", "#6f42c1"]


def load_data(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # Keep empty string for unknown brand — replace only when displaying
    df["brand"] = df["brand"].fillna("")
    df["category"] = df["category"].fillna("unknown")
    df["price_segment"] = df["price_segment"].fillna("low-priced")
    return df


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_figure(fig: plt.Figure, filename: str) -> None:
    filepath = OUTPUT_DIR / filename
    fig.tight_layout()
    fig.savefig(filepath, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {filepath}")


# ── Plot 1: price segment bar chart ──────────────────────────────────────────


def plot_price_segment_counts(df: pd.DataFrame) -> None:
    counts = (
        df["price_segment"].value_counts().reindex(SEGMENT_ORDER).fillna(0).astype(int)
    )
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = counts.plot.bar(color=SEGMENT_COLORS, ax=ax, width=0.6)
    ax.set_title("Количество товаров по ценовым сегментам", fontsize=14, pad=12)
    ax.set_xlabel("Ценовой сегмент")
    ax.set_ylabel("Количество товаров")
    ax.set_xticklabels(SEGMENT_ORDER, rotation=0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis="y", alpha=0.3)

    # Add value labels on top of each bar
    for patch, val in zip(ax.patches, counts):
        ax.text(
            patch.get_x() + patch.get_width() / 2,
            patch.get_height() + counts.max() * 0.01,
            f"{val:,}",
            ha="center",
            va="bottom",
            fontsize=10,
        )
    save_figure(fig, "price_segment_counts.png")


# ── Plot 2: top brands ────────────────────────────────────────────────────────


def plot_top_brands(df: pd.DataFrame, top_n: int = 12) -> None:
    top_brands = df["brand"].replace("", "Unknown").value_counts().head(top_n)
    # Filter out "Unknown" to keep the chart meaningful
    top_brands = top_brands[top_brands.index != "Unknown"].head(top_n)

    fig, ax = plt.subplots(figsize=(9, 5))
    top_brands.plot.barh(color="#17a2b8", ax=ax, width=0.7)
    ax.invert_yaxis()
    ax.set_title(f"Топ {len(top_brands)} брендов в выборке", fontsize=14, pad=12)
    ax.set_xlabel("Количество товаров")
    ax.set_ylabel("")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis="x", alpha=0.3)

    for patch in ax.patches:
        w = patch.get_width()
        ax.text(
            w + top_brands.max() * 0.005,
            patch.get_y() + patch.get_height() / 2,
            f"{int(w):,}",
            va="center",
            fontsize=9,
        )
    save_figure(fig, "top_brands.png")


# ── Plot 3: category distribution ────────────────────────────────────────────


def plot_category_distribution(df: pd.DataFrame, top_n: int = 12) -> None:
    top_categories = df["category"].value_counts().head(top_n)
    fig, ax = plt.subplots(figsize=(10, 5))
    top_categories.plot.bar(color="#fd7e14", ax=ax, width=0.7)
    ax.set_title(f"Топ {top_n} категорий товаров", fontsize=14, pad=12)
    ax.set_xlabel("")
    ax.set_ylabel("Количество товаров")
    ax.tick_params(axis="x", labelrotation=35)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis="y", alpha=0.3)
    save_figure(fig, "top_categories.png")


# ── Plot 4: price distribution histogram ─────────────────────────────────────


def plot_price_distribution(df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    # Full distribution
    df["price_kzt"].plot.hist(bins=60, color="#007bff", alpha=0.8, ax=axes[0])
    axes[0].set_title("Все цены, KZT", fontsize=12)
    axes[0].set_xlabel("Цена, KZT")
    axes[0].set_ylabel("Частота")
    axes[0].xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{int(x/1000)}k")
    )
    axes[0].grid(axis="y", alpha=0.3)

    # Zoom in on 0–1 000 000 KZT for readability
    filtered = df[df["price_kzt"] <= 1_000_000]["price_kzt"]
    filtered.plot.hist(bins=60, color="#6f42c1", alpha=0.8, ax=axes[1])
    axes[1].set_title("Цены до 1 000 000 KZT", fontsize=12)
    axes[1].set_xlabel("Цена, KZT")
    axes[1].set_ylabel("Частота")
    axes[1].xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{int(x/1000)}k")
    )
    axes[1].grid(axis="y", alpha=0.3)

    fig.suptitle("Распределение цен товаров", fontsize=14, y=1.02)
    save_figure(fig, "price_distribution_histogram.png")


# ── Plot 5: outlier share pie ─────────────────────────────────────────────────


def plot_outlier_share(df: pd.DataFrame) -> None:
    outlier_col = df["price_outlier_iqr"].fillna(False).astype(bool)
    n_outliers = int(outlier_col.sum())
    n_normal = len(df) - n_outliers

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.pie(
        [n_normal, n_outliers],
        labels=[f"Не аномалия ({n_normal:,})", f"Аномалия ({n_outliers:,})"],
        autopct="%.1f%%",
        colors=["#198754", "#dc3545"],
        startangle=90,
        wedgeprops={"edgecolor": "white", "linewidth": 1.5},
    )
    ax.set_title("Доля ценовых аномалий (метод 1.5×IQR)", fontsize=13, pad=12)
    save_figure(fig, "outlier_share.png")


# ── Plot 6: avg price by brand (top brands with 3+ items) ────────────────────


def plot_avg_price_by_brand(df: pd.DataFrame, top_n: int = 12) -> None:
    brand_stats = (
        df[df["brand"].ne("")]
        .groupby("brand")["price_kzt"]
        .agg(["mean", "count"])
        .query("count >= 3")
        .sort_values("mean", ascending=False)
        .head(top_n)
    )
    if brand_stats.empty:
        return

    fig, ax = plt.subplots(figsize=(10, 5))
    brand_stats["mean"].plot.bar(color="#e83e8c", ax=ax, width=0.7)
    ax.set_title(
        f"Средняя цена по брендам (топ {len(brand_stats)}), KZT", fontsize=13, pad=12
    )
    ax.set_xlabel("")
    ax.set_ylabel("Средняя цена, KZT")
    ax.tick_params(axis="x", labelrotation=35)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x/1000)}k"))
    ax.grid(axis="y", alpha=0.3)
    save_figure(fig, "avg_price_by_brand.png")


# ── Summary text ─────────────────────────────────────────────────────────────


def create_summary(df: pd.DataFrame) -> None:
    output_path = OUTPUT_DIR / "analysis_summary.txt"
    with output_path.open("w", encoding="utf-8") as f:
        f.write("QazaqPrice analysis summary\n")
        f.write("===========================\n\n")
        f.write(f"Всего строк:              {len(df):,}\n")
        f.write(
            f"Уникальных брендов:       {df['brand'].replace('', 'Unknown').nunique()}\n"
        )
        f.write(f"Уникальных категорий:     {df['category'].nunique()}\n")
        f.write(
            f"Аномалий цен (1.5×IQR):   {int(df['price_outlier_iqr'].fillna(False).sum()):,}\n"
        )
        f.write(f"Мин. цена, KZT:           {df['price_kzt'].min():,.0f}\n")
        f.write(f"Макс. цена, KZT:          {df['price_kzt'].max():,.0f}\n")
        f.write(f"Медиана цены, KZT:        {df['price_kzt'].median():,.0f}\n\n")

        f.write("Ценовые сегменты:\n")
        for seg in SEGMENT_ORDER:
            cnt = int((df["price_segment"] == seg).sum())
            pct = cnt / len(df) * 100
            f.write(f"  {seg:<18} {cnt:>6,}  ({pct:.1f}%)\n")

        f.write("\nТоп брендов:\n")
        for brand, cnt in (
            df["brand"].replace("", "Unknown").value_counts().head(15).items()
        ):
            f.write(f"  {brand:<20} {cnt:>6,}\n")

        f.write("\nТоп категорий:\n")
        for cat, cnt in df["category"].value_counts().head(12).items():
            f.write(f"  {cat:<40} {cnt:>6,}\n")

        f.write("\nПо источникам:\n")
        for src, cnt in df["source"].value_counts().items():
            f.write(f"  {src:<45} {cnt:>6,}\n")

    print(f"Saved: {output_path}")


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    if not SOURCE_CSV.exists():
        raise FileNotFoundError(f"Dataset not found: {SOURCE_CSV.resolve()}")

    ensure_output_dir(OUTPUT_DIR)
    df = load_data(SOURCE_CSV)

    print(f"Loaded {len(df):,} rows.")
    print(df["price_segment"].value_counts())

    plot_price_segment_counts(df)
    plot_top_brands(df)
    plot_category_distribution(df)
    plot_price_distribution(df)
    plot_outlier_share(df)
    plot_avg_price_by_brand(df)
    create_summary(df)

    print("\nAll plots saved to:", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    main()
