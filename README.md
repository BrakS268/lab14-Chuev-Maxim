# lab14-Chuev-Maxim

**Лабораторная работа 14**

**Студент:** Чуев Максим Сергеевич 

**Группа:** 220032-11 

**Вариант:** 29

**Сложность:** средняя


---

## Архитектура конвейера

```
Go-сборщик
  ├─ 5 горутин (датчики полей)    ──►  data/sensors_field-XX.json
  └─ 5 горутин (метеостанции)     ──►  data/weather_<region>.json
        │
        │  NDJSON (один объект на строку)
        ▼
Python — pipeline.py
  ├─ Task 4  Polars: загрузка, схема, статистика
  ├─ Task 5  Polars: очистка, валидация, типы
  ├─ Task 6  Polars: агрегации (GROUP BY field/region)
  ├─ Task 7  Polars: сохранение → data/agro_clean.parquet
  ├─ Task 8  DuckDB: SQL-запрос к Parquet + сравнение с Polars
  └─ Task 9  Plotly: 4 графика → plots/
```

---

## Структура репозитория

```
.
├── collector/
│   ├── main.go       # Go-сборщик (горутины, BatchWriter, graceful shutdown)
│   └── go.mod
├── data/             # NDJSON-файлы (генерируются Go), Parquet
├── plots/            # HTML-графики (генерируются Python)
├── pipeline.py       # Python-конвейер (задания 4–9)
├── PROMPT_LOG.md
└── README.md
```

---

## Запуск

### 1. Go-сборщик

```bash
cd collector
go run main.go
```

Создаёт 10 файлов в `data/`: `sensors_field-0X.json` и `weather_<region>.json`.  
Для остановки нажмите `Ctrl+C` — сборщик выполнит graceful shutdown (дозапишет буфер).

### 2. Python-конвейер

```bash
pip install polars duckdb plotly pandas
python pipeline.py
```

Последовательно выполняет задания 4–9, выводит результаты в терминал и сохраняет:
- `data/agro_clean.parquet` — очищенные данные датчиков
- `data/weather_clean.parquet` — очищенные данные метеостанций
- `plots/*.html` — интерактивные графики

---

## Примеры SQL-запросов (DuckDB)

```sql
-- Средние показатели по полям, отфильтрованные
SELECT field_id,
       AVG(temperature_c)     AS avg_temp,
       AVG(humidity_pct)      AS avg_humidity,
       AVG(soil_moisture_pct) AS avg_soil_moisture,
       AVG(ph)                AS avg_ph,
       COUNT(*)               AS records
FROM read_parquet('data/agro_clean.parquet')
WHERE temperature_c > 10
  AND humidity_pct  > 40
GROUP BY field_id
ORDER BY avg_temp DESC;
```

```sql
-- Поля с повышенной кислотностью почвы
SELECT field_id, AVG(ph) AS avg_ph, COUNT(*) AS n
FROM read_parquet('data/agro_clean.parquet')
WHERE ph < 6.0
GROUP BY field_id
ORDER BY avg_ph;
```

---

## Графики

| Файл | Описание |
|---|---|
| `plots/temp_timeseries.html` | Временной ряд температуры по 5 полям (168 ч) |
| `plots/humidity_histograms.html` | Гистограммы влажности воздуха и почвы |
| `plots/rainfall_pie.html` | Круговая диаграмма осадков по регионам |
| `plots/correlation_heatmap.html` | Тепловая карта корреляций датчиков |

---

## Производительность (DuckDB vs Polars)

На датасете 840 записей (168 ч × 5 полей):

| Движок | Время запроса |
|---|---|
| DuckDB | ~2–5 ms |
| Polars | ~3–8 ms |

DuckDB показывает преимущество на больших Parquet-файлах благодаря колоночному чтению и предикатному пушдауну.

---

## Graceful Shutdown

Go-сборщик перехватывает `SIGINT` / `SIGTERM`. При получении сигнала:
1. Отменяется контекст (`context.WithCancel`) — горутины-генераторы прерывают цикл
2. Уже собранные записи отправляются в `BatchWriter`
3. `BatchWriter.Close()` выполняет финальный flush буфера на диск
4. `sync.WaitGroup` гарантирует завершение всех горутин перед выходом
