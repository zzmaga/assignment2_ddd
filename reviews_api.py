"""
Review collection via public APIs (requires API keys).

Goal (Assignment: Sentiment analysis):
- Collect ratings + review texts for competitors (>=3 companies) from:
  - 2GIS (Places / Reviews)
  - Google Places (Place Details + Reviews)
  - Twitter/X (official API v2 recent search) OR another review source you have access to

This file intentionally keeps credentials out of the repo. Use environment variables.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd
import requests


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Review:
    source: str
    competitor: str
    rating: float | None
    text: str
    created_at: str | None
    url: str | None
    collected_at: str


def fetch_2gis_reviews(
    *,
    api_key: str,
    firm_id: str,
    competitor: str,
    city: str = "almaty",
    max_pages: int = 5,
    page_size: int = 50,
    pause_s: float = 0.2,
) -> list[Review]:
    """
    2GIS API notes:
    - Endpoints and fields can differ by plan/version.
    - This function is written defensively; adjust endpoint/fields if your key uses a different API surface.
    """
    base = "https://catalog.api.2gis.com/3.0"
    out: list[Review] = []

    for page in range(1, max_pages + 1):
        # Common pattern for 2GIS: /items/{id}/reviews
        url = f"{base}/items/{firm_id}/reviews"
        params = {
            "key": api_key,
            "page": page,
            "page_size": page_size,
            "fields": "items.text,items.rating,items.date_created,items.url",
            "locale": "ru_KZ",
            "region_id": city,
        }
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 404:
            # Some accounts use another path; fail fast with a clear hint.
            raise RuntimeError(
                "2GIS reviews endpoint returned 404. "
                "Check your API version/plan and update fetch_2gis_reviews() endpoint."
            )
        r.raise_for_status()
        payload = r.json() or {}
        items = (payload.get("result") or {}).get("items") or (payload.get("items") or [])
        if not items:
            break

        for it in items:
            text = str(it.get("text") or "").strip()
            if not text:
                continue
            rating = it.get("rating")
            try:
                rating_f = float(rating) if rating is not None else None
            except (TypeError, ValueError):
                rating_f = None
            out.append(
                Review(
                    source="2gis",
                    competitor=competitor,
                    rating=rating_f,
                    text=text,
                    created_at=it.get("date_created") or it.get("created_at"),
                    url=it.get("url"),
                    collected_at=_utc_now_iso(),
                )
            )

        time.sleep(pause_s)

    return out


def fetch_google_place_reviews(
    *,
    api_key: str,
    place_id: str,
    competitor: str,
    language: str = "ru",
) -> list[Review]:
    """
    Google Places API:
    - Place Details returns up to a limited number of reviews.
    - For large-scale data you typically need multiple places or another provider.
    """
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "key": api_key,
        "place_id": place_id,
        "language": language,
        "fields": "name,rating,reviews,url",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json() or {}
    result = payload.get("result") or {}
    reviews = result.get("reviews") or []
    place_url = result.get("url")

    out: list[Review] = []
    for rev in reviews:
        text = str(rev.get("text") or "").strip()
        if not text:
            continue
        try:
            rating_f = float(rev.get("rating")) if rev.get("rating") is not None else None
        except (TypeError, ValueError):
            rating_f = None
        out.append(
            Review(
                source="google_places",
                competitor=competitor,
                rating=rating_f,
                text=text,
                created_at=rev.get("time") or rev.get("relative_time_description"),
                url=place_url,
                collected_at=_utc_now_iso(),
            )
        )
    return out


def fetch_twitter_recent(
    *,
    bearer_token: str,
    query: str,
    competitor: str,
    max_results: int = 100,
    pause_s: float = 1.0,
) -> list[Review]:
    """
    Twitter/X API v2 recent search (requires paid/approved access on many accounts).
    We store tweets as "reviews" with rating=None.
    """
    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {bearer_token}"}
    params = {
        "query": query,
        "max_results": max(10, min(100, int(max_results))),
        "tweet.fields": "created_at,lang",
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json() or {}
    tweets = data.get("data") or []

    out: list[Review] = []
    for tw in tweets:
        text = str(tw.get("text") or "").strip()
        if not text:
            continue
        out.append(
            Review(
                source="twitter",
                competitor=competitor,
                rating=None,
                text=text,
                created_at=tw.get("created_at"),
                url=None,
                collected_at=_utc_now_iso(),
            )
        )

    time.sleep(pause_s)
    return out


def reviews_to_df(reviews: Iterable[Review]) -> pd.DataFrame:
    return pd.DataFrame([r.__dict__ for r in reviews])


def save_reviews_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Saved: {path} ({len(df):,} rows)")


def example_run() -> None:
    """
    Example configuration (fill your competitor IDs):
    - 2GIS: firm_id is a 2GIS item id (place/company).
    - Google: place_id from Google Places.
    - Twitter: query like '"Apple Store" (Almaty OR Алматы) -is:retweet lang:ru'
    """
    reviews: list[Review] = []

    key_2gis = os.getenv("DGIS_API_KEY", "").strip()
    key_google = os.getenv("GOOGLE_PLACES_API_KEY", "").strip()
    twitter_bearer = os.getenv("TWITTER_BEARER_TOKEN", "").strip()

    # --- competitors (edit) ---
    competitors = [
        {
            "name": "Apple (official store / reseller)",
            "dgis_firm_id": "",
            "google_place_id": "",
            "twitter_query": '"Apple" ("Алматы" OR "Almaty") lang:ru -is:retweet',
        },
        {
            "name": "Samsung (official store / reseller)",
            "dgis_firm_id": "",
            "google_place_id": "",
            "twitter_query": '"Samsung" ("Алматы" OR "Almaty") lang:ru -is:retweet',
        },
        {
            "name": "Xiaomi (official store / reseller)",
            "dgis_firm_id": "",
            "google_place_id": "",
            "twitter_query": '"Xiaomi" ("Алматы" OR "Almaty") lang:ru -is:retweet',
        },
    ]

    for c in competitors:
        if key_2gis and c["dgis_firm_id"]:
            reviews += fetch_2gis_reviews(
                api_key=key_2gis, firm_id=c["dgis_firm_id"], competitor=c["name"]
            )
        if key_google and c["google_place_id"]:
            reviews += fetch_google_place_reviews(
                api_key=key_google, place_id=c["google_place_id"], competitor=c["name"]
            )
        if twitter_bearer and c["twitter_query"]:
            reviews += fetch_twitter_recent(
                bearer_token=twitter_bearer,
                query=c["twitter_query"],
                competitor=c["name"],
                max_results=100,
            )

    df = reviews_to_df(reviews)
    if df.empty:
        print(
            "No reviews collected. Set API keys + competitor IDs in example_run() "
            "or call fetch_* functions directly."
        )
        return
    os.makedirs("data", exist_ok=True)
    save_reviews_csv(df, "data/reviews_raw.csv")


if __name__ == "__main__":
    example_run()

