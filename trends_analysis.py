"""
Google Trends time series (2005–2025), stationarity + ACF, forecast horizon=10.

Uses pytrends (unofficial API wrapper).

Outputs:
  - data/trends_<term>.csv
  - plots_trends/ (time series, ACF, forecast)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from pytrends.request import TrendReq
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.stattools import adfuller


PLOTS_DIR = Path("plots_trends")
DATA_DIR = Path("data")


@dataclass(frozen=True)
class TrendsConfig:
    term: str
    geo: str  # e.g. "KZ" or "" for worldwide
    start: str  # "2005-01-01"
    end: str  # "2025-12-31"
    freq: str  # "W" (weekly) or "M" (monthly)


def _safe_filename(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9а-яё]+", "_", s, flags=re.IGNORECASE)
    return s.strip("_") or "term"


def fetch_trends(cfg: TrendsConfig) -> pd.Series:
    pytrends = TrendReq(hl="ru-RU", tz=0)
    timeframe = f"{cfg.start} {cfg.end}"
    pytrends.build_payload([cfg.term], timeframe=timeframe, geo=cfg.geo)
    df = pytrends.interest_over_time()
    if df.empty or cfg.term not in df.columns:
        raise RuntimeError("Google Trends returned empty series. Try another term/geo.")
    s = df[cfg.term].copy()
    s.name = "interest"
    # Drop "isPartial" column if present (we only keep interest)
    s.index = pd.to_datetime(s.index)
    return s


def to_monthly(s: pd.Series, freq: str) -> pd.Series:
    if freq.upper().startswith("M"):
        # pandas 3.x: use "ME" (month-end) instead of deprecated "M"
        return s.resample("ME").mean()
    return s  # keep weekly


def stationarity_report(s: pd.Series) -> str:
    s2 = s.dropna()
    res = adfuller(s2.values, autolag="AIC")
    return (
        "ADF stationarity test\n"
        "=====================\n"
        f"ADF statistic: {res[0]:.4f}\n"
        f"p-value:       {res[1]:.6f}\n"
        f"lags used:     {res[2]}\n"
        f"nobs:          {res[3]}\n"
        "critical values:\n"
        + "\n".join([f"  {k}: {v:.4f}" for k, v in res[4].items()])
        + "\n"
    )


def plot_series(s: pd.Series, title: str, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 4.8))
    ax.plot(s.index, s.values, color="#0d6efd", linewidth=1.2)
    ax.set_title(title)
    ax.set_xlabel("Дата")
    ax.set_ylabel("Interest (0–100)")
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_acf_figure(s: pd.Series, out_path: Path, lags: int = 52) -> None:
    fig, ax = plt.subplots(figsize=(12, 4.5))
    plot_acf(s.dropna(), lags=lags, ax=ax)
    ax.set_title("ACF (автокорреляционная функция)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def forecast_sarimax(s: pd.Series, horizon: int = 10) -> pd.DataFrame:
    """
    Simple baseline forecast:
    - SARIMAX(1,1,1) without seasonality (works as a generic baseline).
    """
    s2 = s.dropna()
    model = SARIMAX(s2, order=(1, 1, 1), enforce_stationarity=False, enforce_invertibility=False)
    fit = model.fit(disp=False)
    fc = fit.get_forecast(steps=horizon)
    mean = fc.predicted_mean
    ci = fc.conf_int()
    out = pd.DataFrame(
        {"forecast": mean, "lower": ci.iloc[:, 0], "upper": ci.iloc[:, 1]},
        index=mean.index,
    )
    return out


def plot_forecast(s: pd.Series, fc: pd.DataFrame, out_path: Path, title: str) -> None:
    fig, ax = plt.subplots(figsize=(12, 4.8))
    ax.plot(s.index, s.values, label="history", color="#0d6efd", linewidth=1.2)
    ax.plot(fc.index, fc["forecast"].values, label="forecast", color="#dc3545", linewidth=1.8)
    ax.fill_between(fc.index, fc["lower"].values, fc["upper"].values, color="#dc3545", alpha=0.15)
    ax.set_title(title)
    ax.set_xlabel("Дата")
    ax.set_ylabel("Interest (0–100)")
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    cfg = TrendsConfig(
        term="смартфон",
        geo="KZ",
        start="2005-01-01",
        end="2025-12-31",
        freq="M",
    )

    DATA_DIR.mkdir(exist_ok=True)
    PLOTS_DIR.mkdir(exist_ok=True)

    s = fetch_trends(cfg)
    s = to_monthly(s, cfg.freq)

    out_csv = DATA_DIR / f"trends_{_safe_filename(cfg.term)}_{cfg.geo or 'WW'}.csv"
    s.to_frame().to_csv(out_csv, encoding="utf-8-sig", index=True)

    plot_series(
        s,
        title=f"Google Trends: '{cfg.term}' ({cfg.geo or 'Worldwide'})",
        out_path=PLOTS_DIR / "trends_timeseries.png",
    )
    plot_acf_figure(s, PLOTS_DIR / "trends_acf.png", lags=36 if cfg.freq == "M" else 52)

    rep = stationarity_report(s)
    (PLOTS_DIR / "stationarity_adf.txt").write_text(rep, encoding="utf-8")

    fc = forecast_sarimax(s, horizon=10)
    plot_forecast(
        s,
        fc,
        PLOTS_DIR / "trends_forecast.png",
        title="Прогноз Google Trends (SARIMAX), горизонт=10",
    )

    print(f"Saved: {out_csv}")
    print(f"Saved plots to: {PLOTS_DIR.resolve()}")


if __name__ == "__main__":
    main()

