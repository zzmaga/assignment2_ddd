"""
Sentiment analysis for competitor reviews (negative / neutral / positive).

Input:
  data/reviews_raw.csv with columns:
    - source, competitor, rating (optional), text, created_at (optional), url (optional), collected_at

Outputs:
  - data/reviews_scored.csv (with sentiment labels + probabilities)
  - plots_sentiment/ (time dynamics + distributions)
  - plots_sentiment/quality_report.txt (quality metrics vs rating-derived labels)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import classification_report, confusion_matrix
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline


DATA_IN = Path("data/reviews_raw.csv")
DATA_OUT = Path("data/reviews_scored.csv")
PLOTS_DIR = Path("plots_sentiment")


LABELS = ["negative", "neutral", "positive"]


def ensure_dirs() -> None:
    Path("data").mkdir(exist_ok=True)
    PLOTS_DIR.mkdir(exist_ok=True)


def load_reviews() -> pd.DataFrame:
    if not DATA_IN.exists():
        raise FileNotFoundError(
            f"Missing {DATA_IN}. First collect reviews via reviews_api.py."
        )
    df = pd.read_csv(DATA_IN)
    if "text" not in df.columns:
        raise ValueError("reviews_raw.csv must contain a 'text' column.")
    df["text"] = df["text"].fillna("").astype(str)
    df = df[df["text"].str.strip().ne("")].copy()
    if "competitor" not in df.columns:
        df["competitor"] = "unknown"
    if "source" not in df.columns:
        df["source"] = "unknown"
    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    else:
        df["rating"] = np.nan
    if "created_at" not in df.columns:
        df["created_at"] = np.nan
    return df


def rating_to_label(r: float | None) -> str | None:
    """
    Weak ground-truth from star ratings (common convention):
      1-2 -> negative, 3 -> neutral, 4-5 -> positive
    """
    if r is None or (isinstance(r, float) and math.isnan(r)):
        return None
    if r <= 2:
        return "negative"
    if r == 3:
        return "neutral"
    return "positive"


def parse_created_at_to_month(value: Any) -> str | None:
    """
    Tries multiple formats. If parsing fails, returns None.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    s = str(value).strip()
    if not s:
        return None

    # common ISO
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m")
    except ValueError:
        pass

    # unix timestamp (google review 'time' sometimes arrives as seconds)
    try:
        if s.isdigit() and len(s) in (10, 13):
            ts = int(s)
            if len(s) == 13:
                ts //= 1000
            dt = datetime.utcfromtimestamp(ts)
            return dt.strftime("%Y-%m")
    except Exception:
        pass

    return None


@dataclass(frozen=True)
class ModelSpec:
    name: str
    hf_id: str


MODEL = ModelSpec(
    name="RuBERT sentiment (3-class)",
    hf_id="blanchefort/rubert-base-cased-sentiment",
)


def score_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Uses a pretrained transformer classifier.
    Note: first run downloads model weights (can take time).
    """
    tok = AutoTokenizer.from_pretrained(MODEL.hf_id)
    mdl = AutoModelForSequenceClassification.from_pretrained(MODEL.hf_id)
    clf = pipeline(
        "text-classification",
        model=mdl,
        tokenizer=tok,
        truncation=True,
        top_k=None,
        device=-1,
    )

    texts = df["text"].tolist()
    preds = clf(texts, batch_size=16)

    # normalize output: list[{label, score}, ...] per text
    pred_label: list[str] = []
    p_neg: list[float] = []
    p_neu: list[float] = []
    p_pos: list[float] = []

    for row in preds:
        # row is a list of dicts
        scores = {d["label"].lower(): float(d["score"]) for d in row}
        # model uses labels like 'negative', 'neutral', 'positive'
        pn = scores.get("negative", 0.0)
        pz = scores.get("neutral", 0.0)
        pp = scores.get("positive", 0.0)
        best = max([("negative", pn), ("neutral", pz), ("positive", pp)], key=lambda x: x[1])[0]
        pred_label.append(best)
        p_neg.append(pn)
        p_neu.append(pz)
        p_pos.append(pp)

    out = df.copy()
    out["sentiment"] = pred_label
    out["p_negative"] = p_neg
    out["p_neutral"] = p_neu
    out["p_positive"] = p_pos
    out["model_name"] = MODEL.name
    out["model_hf_id"] = MODEL.hf_id
    return out


def evaluate_quality(df: pd.DataFrame) -> str:
    df_eval = df.copy()
    df_eval["label_from_rating"] = df_eval["rating"].apply(rating_to_label)
    df_eval = df_eval[df_eval["label_from_rating"].notna()].copy()
    if df_eval.empty:
        return "Quality evaluation skipped: no ratings present in the dataset.\n"

    y_true = df_eval["label_from_rating"].tolist()
    y_pred = df_eval["sentiment"].tolist()

    rep = classification_report(
        y_true, y_pred, labels=LABELS, digits=3, zero_division=0
    )
    cm = confusion_matrix(y_true, y_pred, labels=LABELS)
    cm_df = pd.DataFrame(cm, index=[f"true_{l}" for l in LABELS], columns=[f"pred_{l}" for l in LABELS])

    return (
        "Quality evaluation vs rating-derived labels (weak supervision)\n"
        "============================================================\n\n"
        f"Rows with ratings used: {len(df_eval):,}\n\n"
        "Classification report:\n"
        f"{rep}\n"
        "Confusion matrix:\n"
        f"{cm_df.to_string()}\n"
    )


def plot_sentiment_share_over_time(df: pd.DataFrame) -> None:
    tmp = df.copy()
    tmp["month"] = tmp["created_at"].apply(parse_created_at_to_month)
    tmp = tmp[tmp["month"].notna()].copy()
    if tmp.empty:
        return

    g = (
        tmp.groupby(["month", "sentiment"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=LABELS, fill_value=0)
        .sort_index()
    )
    share = g.div(g.sum(axis=1), axis=0)

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(share.index, share["negative"], label="negative", color="#dc3545")
    ax.plot(share.index, share["neutral"], label="neutral", color="#6c757d")
    ax.plot(share.index, share["positive"], label="positive", color="#198754")
    ax.set_title("Доля тональностей по месяцам")
    ax.set_xlabel("Месяц")
    ax.set_ylabel("Доля")
    ax.set_ylim(0, 1)
    ax.grid(alpha=0.25)
    ax.legend()
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "sentiment_share_over_time.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_avg_rating_over_time(df: pd.DataFrame) -> None:
    if df["rating"].isna().all():
        return
    tmp = df.copy()
    tmp["month"] = tmp["created_at"].apply(parse_created_at_to_month)
    tmp = tmp[tmp["month"].notna() & tmp["rating"].notna()].copy()
    if tmp.empty:
        return
    g = tmp.groupby("month")["rating"].mean().sort_index()
    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.plot(g.index, g.values, color="#0d6efd")
    ax.set_title("Средняя оценка (rating) по месяцам")
    ax.set_xlabel("Месяц")
    ax.set_ylabel("Средний rating")
    ax.set_ylim(0, 5)
    ax.grid(alpha=0.25)
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "avg_rating_over_time.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_sentiment_by_competitor(df: pd.DataFrame) -> None:
    g = (
        df.groupby(["competitor", "sentiment"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=LABELS, fill_value=0)
    )
    if g.empty:
        return
    share = g.div(g.sum(axis=1), axis=0).sort_index()

    fig, ax = plt.subplots(figsize=(11, 5))
    y = np.arange(len(share.index))
    left = np.zeros(len(share.index))
    colors = {"negative": "#dc3545", "neutral": "#6c757d", "positive": "#198754"}
    for lab in LABELS:
        ax.barh(y, share[lab].values, left=left, label=lab, color=colors[lab])
        left += share[lab].values
    ax.set_yticks(y)
    ax.set_yticklabels(share.index)
    ax.set_xlim(0, 1)
    ax.set_xlabel("Доля")
    ax.set_title("Распределение тональности по конкурентам")
    ax.legend()
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "sentiment_by_competitor.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    ensure_dirs()
    df = load_reviews()
    df_scored = score_sentiment(df)

    df_scored.to_csv(DATA_OUT, index=False, encoding="utf-8-sig")
    print(f"Saved: {DATA_OUT} ({len(df_scored):,} rows)")

    quality = evaluate_quality(df_scored)
    (PLOTS_DIR / "quality_report.txt").write_text(quality, encoding="utf-8")
    print(f"Saved: {PLOTS_DIR / 'quality_report.txt'}")

    plot_sentiment_share_over_time(df_scored)
    plot_avg_rating_over_time(df_scored)
    plot_sentiment_by_competitor(df_scored)
    print(f"Done. Plots in: {PLOTS_DIR.resolve()}")


if __name__ == "__main__":
    main()

