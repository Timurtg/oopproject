#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Что делает:
  1. Для каждого из 15 городов России скачивает ПОЧАСОВЫЕ данные за выбранный период:
       - качество воздуха (PM2.5, PM10, NO2, SO2, O3, CO, индексы AQI)
         из Open-Meteo Air Quality API (модель CAMS, Copernicus);
       - погоду (температура, влажность, осадки, ветер, давление)
         из Open-Meteo Historical Weather API (реанализ ERA5).
  2. Склеивает всё в одну таблицу, считает суточные агрегаты и флаги превышения норм ВОЗ.
  3. Сохраняет результат во всех удобных форматах:
       CSV, JSON, XLSX, Parquet, SQLite, NumPy (.npz) + «сырые» ответы API.
  4. Пишет словарь данных, отчёт о пропусках и README с описанием набора.

Запуск:
    pip install -r requirements.txt
    python collect_data.py

Ключи и регистрация не нужны. Уже скачанные куски кэшируются в data/raw/,
поэтому при обрыве интернета достаточно запустить скрипт ещё раз.

Данные: Open-Meteo.com (CC BY 4.0), CAMS / Copernicus, ERA5 / ECMWF.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# ============================================================================
# НАСТРОЙКИ
# ============================================================================

AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WX_URL = "https://archive-api.open-meteo.com/v1/archive"

# 12 полных месяцев: по одному полному циклу всех сезонов.
# Конец периода специально отодвинут от «сегодня», потому что у реанализа ERA5
# (погода) есть задержка в несколько дней.
DEFAULT_START = "2025-10-01"
DEFAULT_END = "2026-09-30"

CHUNK_DAYS = 92          # длина одного запроса (чтобы запросы были небольшими)
DEFAULT_DOMAIN = "cams_global"  # одна и та же модель для ВСЕХ городов (сравнимость!)
ALL_FORMATS = ("csv", "json", "xlsx", "parquet", "sqlite", "npz")

# Нормы ВОЗ (рекомендации 2021 года), суточные, мкг/м3. При желании поменяй.
WHO_PM25_24H = 15.0
WHO_PM10_24H = 45.0

# (slug, город, регион, федеральный округ, широта, долгота)
CITIES = [
    ("ufa", "Уфа", "Башкортостан", "ПФО", 54.7388, 55.9721),
    ("moscow", "Москва", "Москва", "ЦФО", 55.7558, 37.6173),
    ("saint_petersburg", "Санкт-Петербург", "Санкт-Петербург", "СЗФО", 59.9343, 30.3351),
    ("kazan", "Казань", "Татарстан", "ПФО", 55.7887, 49.1221),
    ("yekaterinburg", "Екатеринбург", "Свердловская область", "УФО", 56.8389, 60.6057),
    ("novosibirsk", "Новосибирск", "Новосибирская область", "СФО", 55.0084, 82.9357),
    ("krasnoyarsk", "Красноярск", "Красноярский край", "СФО", 56.0153, 92.8932),
    ("chelyabinsk", "Челябинск", "Челябинская область", "УФО", 55.1644, 61.4368),
    ("magnitogorsk", "Магнитогорск", "Челябинская область", "УФО", 53.4186, 59.0472),
    ("norilsk", "Норильск", "Красноярский край", "СФО", 69.3558, 88.1893),
    ("nizhny_novgorod", "Нижний Новгород", "Нижегородская область", "ПФО", 56.3269, 44.0059),
    ("samara", "Самара", "Самарская область", "ПФО", 53.1959, 50.1002),
    ("sochi", "Сочи", "Краснодарский край", "ЮФО", 43.5855, 39.7231),
    ("vladivostok", "Владивосток", "Приморский край", "ДФО", 43.1155, 131.8855),
    ("kaliningrad", "Калининград", "Калининградская область", "СЗФО", 54.7104, 20.4522),
]

# Переменные API -> короткие имена колонок в наших таблицах
AQ_RENAME = {
    "pm10": "pm10",
    "pm2_5": "pm2_5",
    "carbon_monoxide": "co",
    "nitrogen_dioxide": "no2",
    "sulphur_dioxide": "so2",
    "ozone": "o3",
    "european_aqi": "aqi_eu",
    "us_aqi": "aqi_us",
}
WX_RENAME = {
    "temperature_2m": "temp_c",
    "relative_humidity_2m": "humidity_pct",
    "precipitation": "precip_mm",
    "wind_speed_10m": "wind_ms",
    "surface_pressure": "pressure_hpa",
}
AQ_COLS = list(AQ_RENAME.values())
WX_COLS = list(WX_RENAME.values())
VALUE_COLS = AQ_COLS + WX_COLS

# Описание переменных: колонка -> (описание, единица по умолчанию, имя в API)
VAR_INFO = {
    "pm2_5": ("PM2.5 - мелкие взвешенные частицы (<= 2,5 мкм)", "мкг/м3", "pm2_5"),
    "pm10": ("PM10 - взвешенные частицы (<= 10 мкм)", "мкг/м3", "pm10"),
    "co": ("Угарный газ CO", "мкг/м3", "carbon_monoxide"),
    "no2": ("Диоксид азота NO2", "мкг/м3", "nitrogen_dioxide"),
    "so2": ("Диоксид серы SO2", "мкг/м3", "sulphur_dioxide"),
    "o3": ("Озон O3", "мкг/м3", "ozone"),
    "aqi_eu": ("Европейский индекс качества воздуха EAQI (больше = хуже)", "баллы", "european_aqi"),
    "aqi_us": ("Американский индекс качества воздуха US AQI (больше = хуже)", "баллы", "us_aqi"),
    "temp_c": ("Температура воздуха на высоте 2 м", "°C", "temperature_2m"),
    "humidity_pct": ("Относительная влажность воздуха на высоте 2 м", "%", "relative_humidity_2m"),
    "precip_mm": ("Осадки", "мм", "precipitation"),
    "wind_ms": ("Скорость ветра на высоте 10 м", "м/с", "wind_speed_10m"),
    "pressure_hpa": ("Атмосферное давление на уровне станции", "гПа", "surface_pressure"),
}

# Какие суточные агрегаты считаем: колонка -> список функций
DAILY_AGG = {
    "pm2_5": ["mean", "max", "min"],
    "pm10": ["mean", "max"],
    "co": ["mean"],
    "no2": ["mean", "max"],
    "so2": ["mean"],
    "o3": ["mean", "max"],
    "aqi_eu": ["mean", "max"],
    "aqi_us": ["mean", "max"],
    "temp_c": ["mean", "min", "max"],
    "humidity_pct": ["mean"],
    "precip_mm": ["sum"],
    "wind_ms": ["mean", "max"],
    "pressure_hpa": ["mean"],
}
AGG_RU = {
    "mean": "среднее за сутки",
    "max": "максимум за сутки",
    "min": "минимум за сутки",
    "sum": "сумма за сутки",
}

SEASONS = {
    12: "зима", 1: "зима", 2: "зима",
    3: "весна", 4: "весна", 5: "весна",
    6: "лето", 7: "лето", 8: "лето",
    9: "осень", 10: "осень", 11: "осень",
}

# Категории US AQI (по максимуму за сутки)
US_AQI_BINS = [-np.inf, 50, 100, 150, 200, 300, np.inf]
US_AQI_LABELS = [
    "Хорошо", "Умеренно", "Вредно для чувствительных групп",
    "Вредно", "Очень вредно", "Опасно",
]


# ============================================================================
# РАБОТА С API
# ============================================================================

class ApiError(RuntimeError):
    """Ошибка обращения к API, которую повторами не исправить."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


def _reason(resp: requests.Response) -> str:
    try:
        return str(resp.json().get("reason", ""))
    except ValueError:
        return resp.text[:200]


def request_json(url: str, params: dict, retries: int = 6) -> dict:
    """GET-запрос с повторами. При лимитах и сбоях сети ждёт и пробует снова."""
    wait = 5
    last = ""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=90)
        except requests.RequestException as exc:
            last = f"сетевая ошибка: {exc}"
        else:
            if resp.status_code == 200:
                return resp.json()
            reason = _reason(resp)
            last = f"HTTP {resp.status_code}: {reason}"
            if resp.status_code == 429:
                low = reason.lower()
                if "hourly" in low or "daily" in low:
                    # лимит на час/сутки - ждать несколько секунд бесполезно
                    raise ApiError(last, status=429)
                wait = max(wait, 60)
            elif 400 <= resp.status_code < 500:
                raise ApiError(last, status=resp.status_code)
        if attempt < retries:
            print(f"    попытка {attempt}/{retries} не удалась ({last}); жду {wait} с")
            time.sleep(wait)
            wait = min(wait * 2, 120)
    raise ApiError(last)


@dataclass
class Settings:
    start: date
    end: date
    out_dir: Path
    domain: str
    pause: float
    refresh: bool
    formats: set


def date_chunks(start: date, end: date, days: int):
    """Режет период на куски по `days` дней: (начало, конец) включительно."""
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        yield cur, nxt
        cur = nxt + timedelta(days=1)


def cached_get(kind: str, url: str, params: dict, slug: str, cfg: Settings) -> dict:
    """Отдаёт ответ API из кэша (data/raw) или скачивает и кладёт в кэш."""
    digest = hashlib.md5(json.dumps(params, sort_keys=True).encode()).hexdigest()[:8]
    raw_dir = cfg.out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{kind}__{slug}__{params['start_date']}__{params['end_date']}__{digest}.json"

    if path.exists() and not cfg.refresh:
        return json.loads(path.read_text(encoding="utf-8"))

    data = request_json(url, params)
    if "hourly" not in data or "time" not in data["hourly"]:
        raise ApiError(f"неожиданный ответ API: {str(data)[:200]}")
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    time.sleep(cfg.pause)
    return data


def fetch_air_quality(slug: str, lat: float, lon: float, s: date, e: date, cfg: Settings) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": s.isoformat(),
        "end_date": e.isoformat(),
        "hourly": ",".join(AQ_RENAME.keys()),
        "timezone": "auto",
        "domains": cfg.domain,
    }
    try:
        return cached_get("aq", AQ_URL, params, slug, cfg)
    except ApiError as exc:
        # Если API не принял параметр domains - один раз переключаемся на auto.
        if exc.status == 400 and "domain" in str(exc).lower() and cfg.domain != "auto":
            print("  ВНИМАНИЕ: API не принял domains=%s, перехожу на domains=auto." % cfg.domain)
            print("  (в этом случае города могут считаться разными моделями - учти в выводах)")
            cfg.domain = "auto"
            params["domains"] = "auto"
            return cached_get("aq", AQ_URL, params, slug, cfg)
        raise


def fetch_weather(slug: str, lat: float, lon: float, s: date, e: date, cfg: Settings) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": s.isoformat(),
        "end_date": e.isoformat(),
        "hourly": ",".join(WX_RENAME.keys()),
        "timezone": "auto",
        "wind_speed_unit": "ms",
    }
    return cached_get("wx", WX_URL, params, slug, cfg)


def hourly_to_frame(data: dict, rename: dict) -> pd.DataFrame:
    df = pd.DataFrame(data["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    df = df.rename(columns=rename)
    for col in df.columns:
        if col != "time":
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def collect_city(idx: int, total: int, city: tuple, cfg: Settings, units: dict):
    """Скачивает воздух + погоду для одного города, возвращает (таблица, метаданные)."""
    slug, name, region, district, lat, lon = city
    chunks = list(date_chunks(cfg.start, cfg.end, CHUNK_DAYS))
    print(f"[{idx}/{total}] {name}: {len(chunks)} кусков x 2 API")

    aq_parts, wx_parts = [], []
    meta = {
        "city": name, "city_en": slug.replace("_", " ").title(), "region": region,
        "federal_district": district, "latitude": lat, "longitude": lon,
    }
    for n, (s, e) in enumerate(chunks, 1):
        aq = fetch_air_quality(slug, lat, lon, s, e, cfg)
        wx = fetch_weather(slug, lat, lon, s, e, cfg)
        aq_parts.append(hourly_to_frame(aq, AQ_RENAME))
        wx_parts.append(hourly_to_frame(wx, WX_RENAME))
        if n == 1:
            meta.update({
                "timezone": aq.get("timezone"),
                "utc_offset_hours": (aq.get("utc_offset_seconds") or 0) / 3600,
                "aq_grid_latitude": aq.get("latitude"),
                "aq_grid_longitude": aq.get("longitude"),
                "wx_grid_latitude": wx.get("latitude"),
                "wx_grid_longitude": wx.get("longitude"),
                "elevation_m": wx.get("elevation"),
            })
            units.update(aq.get("hourly_units", {}))
            units.update(wx.get("hourly_units", {}))
        print(f"    кусок {n}/{len(chunks)}: {s} .. {e}")

    aq_df = pd.concat(aq_parts).drop_duplicates("time").sort_values("time")
    wx_df = pd.concat(wx_parts).drop_duplicates("time").sort_values("time")
    df = aq_df.merge(wx_df, on="time", how="left")
    for col in VALUE_COLS:                      # на случай, если API не вернул колонку
        if col not in df:
            df[col] = np.nan
    df.insert(0, "city", name)
    return df, meta


# ============================================================================
# ПОДГОТОВКА ТАБЛИЦ
# ============================================================================

def add_calendar(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Добавляет календарные колонки (год, месяц, день недели, сезон...)."""
    ts = df[source]
    df["year"] = ts.dt.year
    df["month"] = ts.dt.month
    df["day"] = ts.dt.day
    df["weekday"] = ts.dt.weekday          # 0 = понедельник
    df["is_weekend"] = (df["weekday"] >= 5).astype(int)
    df["season"] = df["month"].map(SEASONS)
    return df


def build_hourly(frames: list) -> pd.DataFrame:
    hourly = pd.concat(frames, ignore_index=True)
    hourly["date"] = hourly["time"].dt.normalize()
    hourly["hour"] = hourly["time"].dt.hour
    hourly = add_calendar(hourly, "time")
    front = ["city", "time", "date", "year", "month", "day", "hour",
             "weekday", "is_weekend", "season"]
    return hourly[front + VALUE_COLS].sort_values(["city", "time"]).reset_index(drop=True)


def build_daily(hourly: pd.DataFrame) -> pd.DataFrame:
    named = {}
    for col, funcs in DAILY_AGG.items():
        for fn in funcs:
            if fn == "sum":   # сумма без превращения «всё пусто» в 0
                named[f"{col}_{fn}"] = (col, lambda s: s.sum(min_count=1))
            else:
                named[f"{col}_{fn}"] = (col, fn)
    named["n_hours"] = ("pm2_5", "count")

    daily = hourly.groupby(["city", "date"], sort=True).agg(**named).reset_index()
    daily = add_calendar(daily, "date")

    # флаги превышения норм ВОЗ (1 - превышено, 0 - нет, пусто - нет данных)
    for col, limit, flag in (("pm2_5_mean", WHO_PM25_24H, "pm2_5_over_who"),
                             ("pm10_mean", WHO_PM10_24H, "pm10_over_who")):
        valid = daily[col].notna()
        daily[flag] = (daily[col] > limit).astype("Int8").where(valid)

    daily["aqi_us_category"] = pd.cut(
        daily["aqi_us_max"], bins=US_AQI_BINS, labels=US_AQI_LABELS
    ).astype("object")

    calendar = ["city", "date", "year", "month", "day", "weekday", "is_weekend", "season"]
    agg_cols = list(named.keys())
    extra = ["pm2_5_over_who", "pm10_over_who", "aqi_us_category"]
    daily = daily[calendar + agg_cols + extra]
    num = daily.select_dtypes("number").columns.difference(
        ["year", "month", "day", "weekday", "is_weekend", "n_hours"])
    daily[num] = daily[num].round(2)
    return daily


def build_quality_report(hourly: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    expected = ((cfg.end - cfg.start).days + 1) * 24
    rows = []
    for city, g in hourly.groupby("city", sort=False):
        row = {
            "city": city,
            "rows_expected": expected,
            "rows_actual": len(g),
            "first_time": g["time"].min().strftime("%Y-%m-%d %H:%M"),
            "last_time": g["time"].max().strftime("%Y-%m-%d %H:%M"),
        }
        for col in VALUE_COLS:
            row[f"{col}_missing_pct"] = round(g[col].isna().mean() * 100, 2)
        rows.append(row)
    return pd.DataFrame(rows)


def build_dictionary(units: dict, cities: pd.DataFrame) -> pd.DataFrame:
    rows = []

    def add(table, column, desc, unit=""):
        rows.append({"table": table, "column": column, "description": desc, "unit": unit})

    def unit_of(col):
        _, default, api_name = VAR_INFO[col]
        return units.get(api_name) or default

    # cities
    cdesc = {
        "city": "Город", "city_en": "Название латиницей", "region": "Регион",
        "federal_district": "Федеральный округ", "latitude": "Широта (мы запрашивали)",
        "longitude": "Долгота (мы запрашивали)", "timezone": "Часовой пояс (IANA)",
        "utc_offset_hours": "Смещение от UTC, часы",
        "aq_grid_latitude": "Широта узла сетки модели качества воздуха",
        "aq_grid_longitude": "Долгота узла сетки модели качества воздуха",
        "wx_grid_latitude": "Широта узла сетки погодного реанализа",
        "wx_grid_longitude": "Долгота узла сетки погодного реанализа",
        "elevation_m": "Высота над уровнем моря по данным API",
    }
    for col in cities.columns:
        add("cities", col, cdesc.get(col, col))

    calendar = {
        "city": "Город", "time": "Местное время начала часа (часовой пояс города)",
        "date": "Местная дата", "year": "Год", "month": "Месяц (1-12)",
        "day": "День месяца", "hour": "Час суток (0-23)",
        "weekday": "День недели: 0 = понедельник ... 6 = воскресенье",
        "is_weekend": "1 - суббота/воскресенье, 0 - будни",
        "season": "Сезон: зима (дек-фев), весна (мар-май), лето (июн-авг), осень (сен-ноя)",
    }
    for col, desc in calendar.items():
        add("hourly", col, desc)
    for col in VALUE_COLS:
        add("hourly", col, VAR_INFO[col][0], unit_of(col))

    for col, desc in calendar.items():
        if col in ("time", "hour"):
            continue
        add("daily", col, desc)
    for col, funcs in DAILY_AGG.items():
        for fn in funcs:
            add("daily", f"{col}_{fn}", f"{VAR_INFO[col][0]}: {AGG_RU[fn]}", unit_of(col))
    add("daily", "n_hours", "Сколько часов суток с валидным PM2.5 (максимум 24)", "часы")
    add("daily", "pm2_5_over_who",
        f"1 - среднесуточный PM2.5 выше нормы ВОЗ ({WHO_PM25_24H:g} мкг/м3), 0 - нет", "флаг")
    add("daily", "pm10_over_who",
        f"1 - среднесуточный PM10 выше нормы ВОЗ ({WHO_PM10_24H:g} мкг/м3), 0 - нет", "флаг")
    add("daily", "aqi_us_category", "Категория US AQI по суточному максимуму индекса", "")
    return pd.DataFrame(rows)


# ============================================================================
# ЭКСПОРТ
# ============================================================================

def as_text_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Для текстовых форматов (CSV/JSON/SQLite): даты -> ISO-строки."""
    out = df.copy()
    if "time" in out:
        out["time"] = out["time"].dt.strftime("%Y-%m-%d %H:%M")
    if "date" in out:
        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def export_csv(tables: dict, out: Path):
    for name, df in tables.items():
        as_text_dates(df).to_csv(out / f"{name}.csv", index=False, encoding="utf-8-sig")


def export_json(tables: dict, out: Path):
    for name, df in tables.items():
        indent = 2 if len(df) < 500 else None
        as_text_dates(df).to_json(out / f"{name}.json", orient="records",
                                  force_ascii=False, indent=indent)


def export_parquet(tables: dict, out: Path):
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        print("  Parquet пропущен: не установлен pyarrow (pip install pyarrow)")
        return False
    for name, df in tables.items():
        df.to_parquet(out / f"{name}.parquet", index=False)
    return True


def export_sqlite(tables: dict, out: Path):
    path = out / "air_quality.sqlite"
    if path.exists():
        path.unlink()
    with sqlite3.connect(path) as con:
        for name, df in tables.items():
            as_text_dates(df).to_sql(name, con, index=False)
        con.execute("CREATE UNIQUE INDEX ux_cities ON cities(city)")
        con.execute("CREATE UNIQUE INDEX ux_hourly ON hourly(city, time)")
        con.execute("CREATE INDEX ix_hourly_date ON hourly(date)")
        con.execute("CREATE UNIQUE INDEX ux_daily ON daily(city, date)")
        con.commit()


def export_xlsx(tables: dict, out: Path, info: pd.DataFrame):
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    path = out / "air_quality_dataset.xlsx"
    sheets = {"README": info}
    for name, df in tables.items():
        if name == "hourly" and len(df) > 1_000_000:
            print("  В XLSX часовая таблица не помещается (лимит Excel ~1 млн строк) - пропущена")
            continue
        sheets[name] = df.copy()
        if "date" in sheets[name]:
            sheets[name]["date"] = sheets[name]["date"].dt.date

    with pd.ExcelWriter(path, engine="openpyxl", datetime_format="yyyy-mm-dd hh:mm",
                        date_format="yyyy-mm-dd") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)
            ws = writer.sheets[name]
            header_font = Font(name="Arial", bold=True, size=10)
            fill = PatternFill("solid", start_color="D9E1F2")
            for cell in ws[1]:
                cell.font = header_font
                cell.fill = fill
                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            for i, col in enumerate(df.columns, 1):
                width = 100 if name == "README" and i == 2 else max(10, min(len(str(col)) + 4, 34))
                if name == "README" and i == 1:
                    width = 28
                ws.column_dimensions[get_column_letter(i)].width = width
            ws.freeze_panes = "A2"
            if name != "README":
                ws.auto_filter.ref = ws.dimensions


def export_npz(hourly: pd.DataFrame, out: Path) -> bool:
    """Сохраняет 3-мерные массивы [город, день, час] для работы через NumPy."""
    cities = list(dict.fromkeys(hourly["city"]))
    first = hourly.pivot(index="time", columns="city", values=VALUE_COLS[0])
    times = first.index
    if len(times) % 24 != 0 or times[0].hour != 0:
        print("  NPZ пропущен: временной ряд не делится на сутки целиком")
        return False
    n_days = len(times) // 24
    arrays = {}
    for col in VALUE_COLS:
        pv = hourly.pivot(index="time", columns="city", values=col).reindex(times)[cities]
        arrays[col] = pv.to_numpy(dtype="float32").T.reshape(len(cities), n_days, 24)
    dates = times[::24].normalize().values.astype("datetime64[D]")
    np.savez_compressed(out / "air_quality_arrays.npz",
                        cities=np.array(cities), dates=dates,
                        hours=np.arange(24), **arrays)
    return True


def write_data_readme(out: Path, cfg: Settings, hourly, daily, cities, dictionary, quality, files):
    lines = [
        "# Датасет «Дышим чисто»",
        "",
        f"Собран: {datetime.now():%Y-%m-%d %H:%M}",
        f"Период: {cfg.start} .. {cfg.end} (местное время городов)",
        f"Городов: {len(cities)}; строк в `hourly`: {len(hourly):,}; строк в `daily`: {len(daily):,}".replace(",", " "),
        f"Модель качества воздуха: Open-Meteo Air Quality API, domains={cfg.domain} (CAMS, Copernicus)",
        "Погода: Open-Meteo Historical Weather API (реанализ ERA5, ECMWF / Copernicus)",
        "",
        "**Важно:** это модельные оценки на сетке, а не показания датчиков в конкретной точке города.",
        "",
        "## Файлы",
        "",
    ]
    for f in files:
        lines.append(f"- {f}")
    lines += ["", "## Источник и лицензия (для слайда)", "",
              "Данные: Open-Meteo.com (CC BY 4.0); CAMS / Copernicus Atmosphere Monitoring Service; "
              "ERA5 / ECMWF / Copernicus Climate Change Service.", "",
              "## Словарь данных", "",
              "| Таблица | Колонка | Описание | Единица |", "|---|---|---|---|"]
    for r in dictionary.itertuples(index=False):
        lines.append(f"| {r.table} | `{r.column}` | {r.description} | {r.unit} |")
    lines += ["", "## Пропуски по городам (%)", ""]
    miss_cols = ["city"] + [c for c in quality.columns if c.endswith("_missing_pct")]
    lines.append("| " + " | ".join(miss_cols) + " |")
    lines.append("|" + "---|" * len(miss_cols))
    for r in quality[miss_cols].itertuples(index=False):
        lines.append("| " + " | ".join(str(v) for v in r) + " |")
    (out / "README_data.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Сбор датасета качества воздуха для проекта «Дышим чисто»")
    p.add_argument("--start", default=DEFAULT_START, help=f"начало периода YYYY-MM-DD (по умолчанию {DEFAULT_START})")
    p.add_argument("--end", default=DEFAULT_END, help=f"конец периода YYYY-MM-DD (по умолчанию {DEFAULT_END})")
    p.add_argument("--out", default="data", help="папка для результата (по умолчанию data)")
    p.add_argument("--formats", default="all", help=f"через запятую из: {','.join(ALL_FORMATS)} или all")
    p.add_argument("--cities", default="all", help="slug городов через запятую (например ufa,moscow) или all")
    p.add_argument("--domain", default=DEFAULT_DOMAIN, help="модель CAMS: cams_global (по умолчанию), cams_europe или auto")
    p.add_argument("--pause", type=float, default=0.5, help="пауза между запросами, секунды")
    p.add_argument("--refresh", action="store_true", help="игнорировать кэш и скачать заново")
    return p.parse_args()


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    args = parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if end < start:
        print("Ошибка: конец периода раньше начала")
        return 2

    formats = set(ALL_FORMATS) if args.formats == "all" else {f.strip().lower() for f in args.formats.split(",")}
    bad = formats - set(ALL_FORMATS)
    if bad:
        print(f"Неизвестные форматы: {', '.join(sorted(bad))}. Допустимо: {', '.join(ALL_FORMATS)}")
        return 2

    selected = CITIES
    if args.cities != "all":
        wanted = [c.strip() for c in args.cities.split(",")]
        known = {c[0] for c in CITIES}
        unknown = [w for w in wanted if w not in known]
        if unknown:
            print(f"Неизвестные города: {', '.join(unknown)}. Доступны: {', '.join(sorted(known))}")
            return 2
        selected = [c for c in CITIES if c[0] in wanted]

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cfg = Settings(start, end, out, args.domain, args.pause, args.refresh, formats)

    if (date.today() - end).days < 7:
        print("Заметка: у погодного реанализа ERA5 задержка несколько дней - "
              "в последних днях периода погода может быть пустой.")

    print(f"Период {start} .. {end}, городов: {len(selected)}, модель воздуха: {cfg.domain}")
    print(f"Кэш и результат: {out.resolve()}\n")

    units: dict = {}
    frames, metas = [], []
    try:
        for i, city in enumerate(selected, 1):
            df, meta = collect_city(i, len(selected), city, cfg, units)
            frames.append(df)
            metas.append(meta)
    except ApiError as exc:
        print(f"\nОШИБКА API: {exc}")
        print("Уже скачанные куски сохранены в кэше - после исправления просто запусти скрипт снова.")
        return 1

    print("\nСобираю таблицы...")
    hourly = build_hourly(frames)
    daily = build_daily(hourly)
    cities = pd.DataFrame(metas)
    dictionary = build_dictionary(units, cities)
    quality = build_quality_report(hourly, cfg)
    tables = {"cities": cities, "hourly": hourly, "daily": daily}
    aux = {"data_dictionary": dictionary, "data_quality_report": quality}

    files = []
    if "csv" in formats:
        export_csv({**tables, **aux}, out)
        files += [f"{n}.csv" for n in (*tables, *aux)]
    if "json" in formats:
        export_json({**tables, **aux}, out)
        files += [f"{n}.json" for n in (*tables, *aux)]
    if "parquet" in formats and export_parquet(tables, out):
        files += [f"{n}.parquet" for n in tables]
    if "sqlite" in formats:
        export_sqlite({**tables, **aux}, out)
        files.append("air_quality.sqlite  (таблицы: cities, hourly, daily, data_dictionary, data_quality_report)")
    if "npz" in formats and export_npz(hourly, out):
        files.append("air_quality_arrays.npz  (массивы [город, день, час])")
    if "xlsx" in formats:
        print("Пишу Excel (часовая таблица большая, это может занять около минуты)...")
        info = pd.DataFrame({
            "Параметр": ["Проект", "Период", "Городов", "Источник воздуха", "Источник погоды", "Лицензия"],
            "Значение": [
                "Дышим чисто - анализ качества воздуха в городах России",
                f"{start} .. {end}", len(cities),
                f"Open-Meteo Air Quality API (CAMS), domains={cfg.domain}",
                "Open-Meteo Historical Weather API (ERA5)",
                "Open-Meteo.com, CC BY 4.0",
            ],
        })
        export_xlsx({**tables, "data_dictionary": dictionary, "data_quality_report": quality}, out, info)
        files.append("air_quality_dataset.xlsx  (листы: README, cities, hourly, daily, data_dictionary, data_quality_report)")

    files.append("raw/  (исходные ответы API в JSON)")
    (out / "collection_metadata.json").write_text(json.dumps({
        "collected_at": datetime.now().isoformat(timespec="seconds"),
        "start": str(start), "end": str(end), "domain": cfg.domain,
        "cities": [c[0] for c in selected], "rows_hourly": len(hourly), "rows_daily": len(daily),
        "air_quality_api": AQ_URL, "weather_api": WX_URL,
        "who_pm25_24h": WHO_PM25_24H, "who_pm10_24h": WHO_PM10_24H,
        "units_from_api": units,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    files.append("collection_metadata.json")
    write_data_readme(out, cfg, hourly, daily, cities, dictionary, quality, files)
    files.append("README_data.md")

    print("\n=== ГОТОВО ===")
    print(f"Часовых строк: {len(hourly):,}; суточных: {len(daily):,}".replace(",", " "))
    print("\nСредний PM2.5 за период, мкг/м3 (быстрая проверка, что данные живые):")
    print(daily.groupby("city")["pm2_5_mean"].mean().round(1).sort_values().to_string())
    worst = quality.filter(like="_missing_pct").max().max()
    print(f"\nМаксимальная доля пропусков в любой колонке: {worst}%  (подробно: data_quality_report.csv)")
    print(f"\nФайлы в {out.resolve()}:")
    for f in files:
        print("  -", f)
    return 0


if __name__ == "__main__":
    sys.exit(main())
