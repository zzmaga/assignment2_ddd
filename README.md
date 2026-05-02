# QazaqPrice — Data Preparation and Market Intelligence

## Цель проекта

Подготовить данные из двух и более источников, очистить их, объединить и провести анализ конкурентной среды по ценовым сегментам.

## Структура проекта

- `pipeline.py` — основной скрипт для загрузки данных из:
  - открытого CSV-источника (`merged_electronics_dataset.csv`),
  - API Escuela JS (`api.escuelajs.co`),
  - живых страниц Apple / Samsung / Google.
- `qazaqprice_dataset.csv` — очищенный результат после объединения данных.
- `data_cleaning_log.txt` — лог всех шагов очистки, удаления дублей и флагов выбросов.
- `analysis.py` — новый скрипт для визуализации и создания базовых «дашбордов».
- `reviews_api.py` — сбор отзывов конкурентов через API (2GIS / Google Places / Twitter).
- `sentiment_analysis.py` — классификация тональности (negative/neutral/positive), оценка качества, динамика.
- `trends_analysis.py` — Google Trends 2005–2025: визуализация ряда, ACF/стационарность, прогноз (горизонт=10).
- `requirements.txt` — зависимости проекта.

## Что уже реализовано

1. Сбор данных из минимум двух источников.
2. Конвертация цен в KZT.
3. Анализ на дубликаты с удалением.
4. Проверка на выбросы по методу 1.5xIQR и пометка аномалий.
5. Создание ценовых сегментов:
   - `low-priced`,
   - `middle-priced`,
   - `high-priced`,
   - `luxury`.
6. Сохранение чистого набора данных в `qazaqprice_dataset.csv`.

## Как сделать визуализацию

Запустите:

```bash
python analysis.py
```

Скрипт создаст папку `plots/` и сохранит там:

- `price_segment_counts.png` — количество товаров по ценовым сегментам.
- `top_brands.png` — топ брендов по числу товаров.
- `top_categories.png` — топ категорий.
- `price_distribution_histogram.png` — распределение цен.
- `outlier_share.png` — доля ценовых аномалий.
- `analysis_summary.txt` — текстовое резюме ключевых показателей.

## Как документировать проделанную работу

1. Используйте `data_cleaning_log.txt` для описания всех шагов очистки:
   - загрузка данных,
   - парсинг цен,
   - удаление пустых записей,
   - работа с дубликатами,
   - определение и пометка выбросов.
2. Добавьте в отчёт информацию о источниках данных и методике:
   - CSV-источник,
   - Escuela JS API,
   - живые страницы официальных магазинов.
3. Включите ключевые метрики:
   - число строк после очистки,
   - число удалённых дублей,
   - число помеченных выбросов,
   - распределение по сегментам.
4. Визуализации превращайте в слайды/отчёт — например, бар-чарты по сегментам, брендам и категориям.

## Что можно добавить в анализ

- SWOT-анализ конкурентов с учётом сегментации.
- Оценку ключевых потребителей:
  - `low-priced` — массовый рынок, бюджетные покупатели,
  - `middle-priced` — активные покупатели со средней покупательной способностью,
  - `high-priced` / `luxury` — «early adopters» и премиальный сегмент.
- Технологический уровень конкурентов можно описать по категориям и брендам:
  - смартфоны и планшеты = высокотехнологичные продукты,
  - аксессуары и аудио = более массовый сегмент.

## Быстрые выводы из текущих данных

- В данных доминирует `low-priced` сегмент (4729 записей),
- `middle-priced` и `high-priced` сегменты представлены значительно меньше.
- Это означает, что перспективный сегмент для вашей идеи может быть следующий:
  - либо продолжать работать в бюджете и фокусироваться на объёме,
  - либо искать нишу в `middle-priced` сегменте, где конкуренция меньше.

## Запуск проекта

```bash
python -m pip install -r requirements.txt
python pipeline.py
python analysis.py
```

---

## Задание: анализ тональности отзывов (Assignment 2 / Sentiment)

### 1) Сбор данных (минимум 3 компании)

В `reviews_api.py` реализованы функции сбора:
- **2GIS**: `fetch_2gis_reviews()` (нужен `DGIS_API_KEY` + `firm_id`)
- **Google Places**: `fetch_google_place_reviews()` (нужен `GOOGLE_PLACES_API_KEY` + `place_id`)
- **Twitter/X**: `fetch_twitter_recent()` (нужен `TWITTER_BEARER_TOKEN`)

Сохранение результата: `data/reviews_raw.csv`.

Примечание: в репозитории лежит **пример** `data/reviews_raw.csv` (несколько строк), чтобы пайплайн запускался сразу. Для сдачи замените его на выгрузку из API по вашим конкурентам.

Запуск (после выставления переменных окружения и заполнения ID конкурентов в `example_run()`):

```bash
python reviews_api.py
```

### 2) Анализ тональности + качество + динамика

Скрипт `sentiment_analysis.py`:
- классифицирует отзывы на **negative / neutral / positive** (модель RuBERT),
- оценивает качество (слабая разметка по рейтингу: 1–2 / 3 / 4–5),
- строит динамику долей тональности и средней оценки по времени.

```bash
python sentiment_analysis.py
```

Выход:
- `data/reviews_scored.csv`
- `plots_sentiment/sentiment_share_over_time.png`
- `plots_sentiment/avg_rating_over_time.png`
- `plots_sentiment/sentiment_by_competitor.png`
- `plots_sentiment/quality_report.txt`

### 3) Дашборд (Power BI / Tableau)

Для дашборда используйте `data/reviews_scored.csv`:
- разрезы: `competitor`, `source`, `sentiment`, `created_at/month`
- метрики: доля негативных, средний rating, топ тем негативных отзывов (фильтр `sentiment=negative`)

---

## Задание: Google Trends (2005–2025)

Скрипт `trends_analysis.py`:
- скачивает ряд по термину (по умолчанию: `смартфон`, geo=`KZ`),
- визуализирует ряд,
- проверяет стационарность (ADF),
- строит ACF,
- прогнозирует на 10 шагов (SARIMAX baseline).

```bash
python trends_analysis.py
```

Выход:
- `data/trends_<term>_<geo>.csv`
- `plots_trends/trends_timeseries.png`
- `plots_trends/trends_acf.png`
- `plots_trends/stationarity_adf.txt`
- `plots_trends/trends_forecast.png`
