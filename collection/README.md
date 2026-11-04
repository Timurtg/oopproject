# Сбор данных

Данные собраны скриптом `collect_data.py` через открытые API Open-Meteo. Сбор охватывает 15 городов России за период с 1 октября 2025 года по 30 сентября 2026 года.

## Источники

- Качество воздуха: [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api), модель CAMS Global / Copernicus.
- Погода: [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), реанализ ERA5 / ECMWF / Copernicus.

Качество воздуха — модельные оценки на сетке, не измерения городских датчиков. Использование данных Open-Meteo регулируется лицензией CC BY 4.0.

## Обработка

Для каждого города скрипт запрашивает почасовые показатели воздуха и погоды. Период загружается частями длиной до 92 дней; часовой пояс устанавливается автоматически для города. Ответы API сохраняются в `data/raw/` и используются как кэш при повторном запуске.

Затем скрипт объединяет показатели, формирует почасовую таблицу и рассчитывает суточные агрегаты. Для PM2.5 и PM10 создаются флаги превышения порогов 15 и 45 мкг/м³ соответственно. Эти флаги относятся к рекомендациям ВОЗ, а не к российским нормативам.

Скрипт может сохранить таблицы в CSV, JSON, Parquet, SQLite, Excel и NumPy. Для текущей проверки в репозитории оставлены Excel-книга и CSV-таблицы.

## Повторный сбор

Выполнять команды из корневой папки репозитория:

```powershell
py -3 -m venv .\collection\.venv
.\collection\.venv\Scripts\python.exe -m pip install -r collection\requirements.txt
.\collection\.venv\Scripts\python.exe collection\collect_data.py --out data
```

По умолчанию используются период и список из 15 городов, указанные в скрипте. Параметры можно посмотреть командой:

```powershell
.\collection\.venv\Scripts\python.exe collection\collect_data.py --help
```
