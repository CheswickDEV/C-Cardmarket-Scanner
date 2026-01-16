# Cardmarket Scanner v2

Automatisierter Preis-Scanner für Cardmarket mit strukturiertem HTML-Parsing, detaillierter Angebotsspeicherung und automatischer Deal-Erkennung.

## Features

- **Strukturiertes HTML-Parsing**: DOM-basiertes Parsing mit BeautifulSoup statt Regex
- **Detaillierte Angebotsspeicherung**: Jedes einzelne Angebot wird mit allen Metadaten gespeichert
- **Statistische Analyse**: Quantile, Median, IQR, Trimmed Mean für robuste Preisanalyse
- **Automatische Deal-Erkennung**: Rolling-Baseline-Vergleich erkennt unterbewertete Angebote
- **Robustes Error-Handling**: Fehler bei einer Karte stoppen nicht den gesamten Scan
- **Skalierbar**: Bulk-Inserts und Connection-Pooling für stündliche Scans

## Schnellstart

### 1. Voraussetzungen

```bash
# Python 3.8+
python3 --version

# MySQL/MariaDB
mysql --version

# FlareSolverr (Docker)
docker run -d \
  --name=flaresolverr \
  -p 8191:8191 \
  ghcr.io/flaresolverr/flaresolverr:latest
```

### 2. Installation

```bash
# Repository klonen
git clone https://github.com/your-repo/C-Cardmarket-Scanner.git
cd C-Cardmarket-Scanner

# Virtual Environment erstellen
python3 -m venv venv
source venv/bin/activate

# Dependencies installieren
pip install mysql-connector-python beautifulsoup4 requests pytest
```

### 3. Datenbank einrichten

```bash
# Datenbank und User erstellen
mysql -u root -p <<EOF
CREATE DATABASE IF NOT EXISTS cardmarket CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'cardmarket'@'localhost' IDENTIFIED BY 'DEIN_PASSWORT';
GRANT ALL PRIVILEGES ON cardmarket.* TO 'cardmarket'@'localhost';
FLUSH PRIVILEGES;
EOF

# Schema importieren
mysql -u cardmarket -p cardmarket < schema.sql
```

### 4. Konfiguration

Erstelle eine `.env`-Datei oder setze Umgebungsvariablen:

```bash
# Datenbank
export DB_HOST=localhost
export DB_PORT=3306
export DB_USER=cardmarket
export DB_PASS=DEIN_PASSWORT
export DB_NAME=cardmarket

# FlareSolverr
export FLARESOLVERR_URL=http://localhost:8191/v1

# Limits
export MAX_OFFERS_PER_SCAN=150
export SLEEP_BETWEEN_CARDS_SEC=2
export REQUEST_TIMEOUT_SEC=120

# Deal-Erkennung
export DEAL_THRESHOLD=0.15          # 15% unter Baseline
export BASELINE_WINDOW_SCANS=48     # Letzte 48 Scans (2 Tage bei stündlich)
export MIN_SELLER_RATING=90.0       # Mindestens 90% Rating
export MIN_CONDITION=GD             # Mindestens "Good"

# Logging
export LOG_LEVEL=INFO
```

### 5. Erste Karten hinzufügen

```bash
# Einzelne Karte hinzufügen
python cron_scanner_v2.py add 257 OGN DE        # Lee Sin Blind Monk, Deutschland
python cron_scanner_v2.py add 257 OGN DE foil   # Foil-Version

# Watchlist anzeigen
python cron_scanner_v2.py list
```

### 6. Ersten Scan durchführen

```bash
# Test einer einzelnen Karte (ohne DB-Speicherung der Watchlist)
python cron_scanner_v2.py test 257 OGN DE

# Vollständiger Scan
python cron_scanner_v2.py scan
```

## Verwendung

### CLI-Befehle

```bash
# Vollständiger Watchlist-Scan
python cron_scanner_v2.py scan

# Einzelne Karte testen
python cron_scanner_v2.py test <nummer> <set> [land] [foil]

# Karte zur Watchlist hinzufügen
python cron_scanner_v2.py add <nummer> <set> [land] [foil]

# Watchlist anzeigen
python cron_scanner_v2.py list

# Aktuelle Deals anzeigen
python cron_scanner_v2.py deals [tage]

# Statistiken für eine Karte
python cron_scanner_v2.py stats <nummer> <set> [land] [tage]
```

### Cronjob einrichten

```bash
# Stündlicher Scan
0 * * * * cd /opt/cardmarket-scanner && /opt/cardmarket-scanner/venv/bin/python cron_scanner_v2.py >> /var/log/cardmarket-scanner.log 2>&1

# Tägliche Datenbereinigung (3:00 Uhr)
0 3 * * * cd /opt/cardmarket-scanner && /opt/cardmarket-scanner/venv/bin/python retention.py --execute >> /var/log/cardmarket-retention.log 2>&1
```

## Datenbank-Schema

### Tabellen

| Tabelle | Beschreibung |
|---------|--------------|
| `scan_run` | Protokoll jedes Scan-Durchlaufs |
| `offer_snapshot` | Einzelne Angebote (N pro Scan) |
| `scan_agg` | Aggregierte Statistiken pro Scan |
| `deal_alert` | Erkannte Deal-Alarme |
| `watchlist` | Zu scannende Karten |
| `preis_historie` | Legacy-Tabelle (Kompatibilität) |

### ER-Diagramm

```
watchlist (1) ----< (n) scan_run (1) ----< (n) offer_snapshot
                         |
                         | (1:1)
                         v
                      scan_agg
                         |
                         | (1:n)
                         v
                    deal_alert
```

## Beispiel-SQL-Abfragen

### Preisband (P10-P90) über Zeit

```sql
-- Preisentwicklung mit Konfidenzband
SELECT
    DATE(sr.ts) as datum,
    sr.card_name,
    AVG(sa.p10_total) as p10,
    AVG(sa.p25_total) as p25,
    AVG(sa.median_total) as median,
    AVG(sa.p75_total) as p75,
    AVG(sa.p90_total) as p90,
    AVG(sa.offer_count) as avg_offers
FROM scan_run sr
INNER JOIN scan_agg sa ON sr.id = sa.scan_id
WHERE sr.ok = 1
AND sr.karten_nummer = '257'
AND sr.set_code = 'OGN'
AND sr.land = 'DE'
AND sr.ts >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY DATE(sr.ts), sr.card_name
ORDER BY datum;
```

### Median vs. Rolling-Median (7 Tage)

```sql
-- Vergleich aktueller Median mit Rolling-Baseline
SELECT
    sr.ts,
    sr.card_name,
    sa.median_total as current_median,
    (
        SELECT AVG(sa2.median_total)
        FROM scan_run sr2
        INNER JOIN scan_agg sa2 ON sr2.id = sa2.scan_id
        WHERE sr2.karten_nummer = sr.karten_nummer
        AND sr2.set_code = sr.set_code
        AND sr2.land = sr.land
        AND sr2.foil = sr.foil
        AND sr2.ok = 1
        AND sr2.ts BETWEEN DATE_SUB(sr.ts, INTERVAL 7 DAY) AND sr.ts
    ) as rolling_median_7d,
    sa.offer_count
FROM scan_run sr
INNER JOIN scan_agg sa ON sr.id = sa.scan_id
WHERE sr.ok = 1
AND sr.karten_nummer = '257'
AND sr.set_code = 'OGN'
ORDER BY sr.ts DESC
LIMIT 100;
```

### Günstigstes Angebot vs. Baseline

```sql
-- Aktuelle günstigste Angebote im Vergleich zur Baseline
WITH latest_scans AS (
    SELECT
        sr.karten_nummer,
        sr.set_code,
        sr.land,
        sr.foil,
        sr.card_name,
        MAX(sr.id) as latest_scan_id
    FROM scan_run sr
    WHERE sr.ok = 1
    GROUP BY sr.karten_nummer, sr.set_code, sr.land, sr.foil, sr.card_name
),
baselines AS (
    SELECT
        sr.karten_nummer,
        sr.set_code,
        sr.land,
        sr.foil,
        AVG(sa.median_total) as baseline
    FROM scan_run sr
    INNER JOIN scan_agg sa ON sr.id = sa.scan_id
    WHERE sr.ok = 1
    AND sr.ts >= DATE_SUB(NOW(), INTERVAL 48 HOUR)
    GROUP BY sr.karten_nummer, sr.set_code, sr.land, sr.foil
)
SELECT
    ls.card_name,
    ls.set_code,
    ls.land,
    IF(ls.foil, 'FOIL', 'Normal') as version,
    sa.min_total as cheapest,
    b.baseline,
    ROUND((sa.min_total - b.baseline) / b.baseline * 100, 1) as diff_pct,
    sa.offer_count
FROM latest_scans ls
INNER JOIN scan_agg sa ON ls.latest_scan_id = sa.scan_id
LEFT JOIN baselines b ON
    ls.karten_nummer = b.karten_nummer
    AND ls.set_code = b.set_code
    AND ls.land = b.land
    AND ls.foil = b.foil
ORDER BY diff_pct ASC
LIMIT 20;
```

### Aktive Deals der letzten 24 Stunden

```sql
SELECT
    da.ts as gefunden,
    da.card_name,
    da.set_code,
    da.land,
    da.total as preis,
    da.baseline,
    ROUND(da.discount_pct * 100, 1) as rabatt_pct,
    da.seller_name,
    da.condition,
    da.article_url
FROM deal_alert da
WHERE da.ts >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
ORDER BY da.discount_pct ASC
LIMIT 50;
```

### Seller-Analyse

```sql
-- Top-Verkäufer nach Angebotshäufigkeit
SELECT
    os.seller_name,
    os.seller_country,
    COUNT(*) as total_offers,
    AVG(os.seller_rating) as avg_rating,
    AVG(os.price_item) as avg_price,
    COUNT(DISTINCT os.scan_id) as scans_with_offers
FROM offer_snapshot os
INNER JOIN scan_run sr ON os.scan_id = sr.id
WHERE sr.ts >= DATE_SUB(NOW(), INTERVAL 7 DAY)
AND os.seller_name IS NOT NULL
GROUP BY os.seller_name, os.seller_country
HAVING total_offers >= 10
ORDER BY total_offers DESC
LIMIT 20;
```

### Volatilitätsanalyse

```sql
-- Karten mit hoher Preisvolatilität
SELECT
    sr.card_name,
    sr.set_code,
    sr.land,
    COUNT(*) as scans,
    MIN(sa.median_total) as min_median,
    MAX(sa.median_total) as max_median,
    AVG(sa.median_total) as avg_median,
    STDDEV(sa.median_total) as stddev_median,
    (MAX(sa.median_total) - MIN(sa.median_total)) / AVG(sa.median_total) * 100 as volatility_pct
FROM scan_run sr
INNER JOIN scan_agg sa ON sr.id = sa.scan_id
WHERE sr.ok = 1
AND sr.ts >= DATE_SUB(NOW(), INTERVAL 14 DAY)
GROUP BY sr.card_name, sr.set_code, sr.land
HAVING scans >= 10
ORDER BY volatility_pct DESC
LIMIT 20;
```

## Datenaufbewahrung (Retention)

Das `retention.py`-Skript bereinigt alte Daten:

```bash
# Dry-Run (zeigt was gelöscht würde)
python retention.py

# Ausführen
python retention.py --execute

# Nur Statistiken anzeigen
python retention.py --stats

# Tabellen optimieren
python retention.py --optimize
```

### Konfiguration

```bash
export RETENTION_OFFERS_DAYS=30      # offer_snapshot: 30 Tage
export RETENTION_AGGREGATES_DAYS=365 # scan_run/agg: 1 Jahr
export RETENTION_DEALS_DAYS=90       # deal_alert: 90 Tage
export RETENTION_LEGACY_DAYS=365     # preis_historie: 1 Jahr
```

## Tests

```bash
# Alle Tests ausführen
python -m pytest tests/ -v

# Nur Parser-Tests
python -m pytest tests/test_parser.py -v

# Mit Coverage
pip install pytest-cov
python -m pytest tests/ --cov=. --cov-report=html
```

## Troubleshooting

### FlareSolverr nicht erreichbar

```bash
# Status prüfen
docker ps | grep flaresolverr

# Logs anzeigen
docker logs flaresolverr

# Neustart
docker restart flaresolverr
```

### Keine Angebote gefunden

1. URL manuell im Browser testen
2. HTML-Struktur von Cardmarket könnte sich geändert haben
3. Parser-Selektoren in `CardmarketParser` anpassen
4. `parse_version` in `cron_scanner_v2.py` hochzählen

### Datenbank-Verbindungsprobleme

```bash
# Verbindung testen
mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p$DB_PASS $DB_NAME -e "SELECT 1"

# Berechtigungen prüfen
mysql -u root -p -e "SHOW GRANTS FOR 'cardmarket'@'localhost'"
```

### Hoher Speicherverbrauch

```bash
# offer_snapshot Tabelle ist zu groß
python retention.py --stats

# Retention ausführen
python retention.py --execute

# Manuell bereinigen
mysql -u cardmarket -p cardmarket <<EOF
DELETE FROM offer_snapshot
WHERE scan_id IN (
    SELECT id FROM scan_run WHERE ts < DATE_SUB(NOW(), INTERVAL 7 DAY)
);
OPTIMIZE TABLE offer_snapshot;
EOF
```

## Architektur

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Watchlist     │────>│  cron_scanner_v2 │────>│   FlareSolverr  │
│   (MySQL)       │     │    (Python)      │     │    (Docker)     │
└─────────────────┘     └────────┬─────────┘     └────────┬────────┘
                                 │                        │
                                 │  HTML                  │ HTTP
                                 v                        v
                        ┌────────────────┐       ┌────────────────┐
                        │  Parser        │<──────│  Cardmarket    │
                        │  (BeautifulSoup)       │  Website       │
                        └────────┬───────┘       └────────────────┘
                                 │
                    ┌────────────┼────────────┐
                    v            v            v
            ┌───────────┐ ┌───────────┐ ┌───────────┐
            │offer_     │ │scan_agg   │ │deal_alert │
            │snapshot   │ │           │ │           │
            └───────────┘ └───────────┘ └───────────┘
```

## Lizenz

MIT License

## Changelog

### v2.0 (2026-01)

- Strukturiertes HTML-Parsing mit BeautifulSoup
- Einzelne Angebote in `offer_snapshot` speichern
- Aggregierte Statistiken (Quantile, Median, IQR)
- Automatische Deal-Erkennung
- Connection-Pooling für bessere Performance
- Umfassende Umgebungsvariablen-Konfiguration
- Retention-Skript für Datenbereinigung
- Unit-Tests mit Fixtures
