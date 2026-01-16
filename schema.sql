-- ============================================
-- Cardmarket Scanner v2 - Database Schema
-- ============================================
-- Migration: Neue Tabellen für erweiterte Preis-Analyse
-- Erfordert MySQL 5.7+ oder MariaDB 10.2+ (für JSON-Support)
--
-- Ausführen mit:
--   mysql -u cardmarket -p cardmarket < schema.sql
-- ============================================

-- ============================================
-- TABELLE: scan_run
-- ============================================
-- Protokolliert jeden einzelnen Scan-Durchlauf pro Watchlist-Eintrag

CREATE TABLE IF NOT EXISTS scan_run (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Scan-Zeitpunkt (UTC)',
    watchlist_id BIGINT UNSIGNED NULL COMMENT 'FK zur watchlist (optional)',
    product_url TEXT NOT NULL COMMENT 'Vollständige Cardmarket-URL',
    product_id VARCHAR(64) NULL COMMENT 'Produkt-ID aus URL/HTML',
    card_name VARCHAR(255) NULL COMMENT 'Kartenname',
    set_code VARCHAR(32) NOT NULL COMMENT 'Set-Kürzel (z.B. OGN, OGS)',
    karten_nummer VARCHAR(64) NOT NULL COMMENT 'Kartennummer im Set',
    land CHAR(2) NOT NULL COMMENT 'Länder-Filter (z.B. DE, AT)',
    foil TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1=Foil, 0=Normal',
    ok TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1=Scan erfolgreich, 0=Fehler',
    http_status INT NULL COMMENT 'HTTP-Statuscode',
    error TEXT NULL COMMENT 'Fehlermeldung bei ok=0',
    parse_version VARCHAR(32) NOT NULL DEFAULT 'v2.0' COMMENT 'Parser-Version',
    PRIMARY KEY (id),
    INDEX idx_scan_run_ts (ts),
    INDEX idx_scan_run_card_ts (karten_nummer, set_code, land, foil, ts),
    INDEX idx_scan_run_watchlist_ts (watchlist_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Scan-Protokoll pro Watchlist-Eintrag und Durchlauf';


-- ============================================
-- TABELLE: offer_snapshot
-- ============================================
-- Speichert einzelne Angebote pro Scan (N pro scan_run)

CREATE TABLE IF NOT EXISTS offer_snapshot (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    scan_id BIGINT UNSIGNED NOT NULL COMMENT 'FK -> scan_run.id',
    position INT UNSIGNED NOT NULL COMMENT 'Sortierposition auf der Seite (1-basiert)',
    article_url TEXT NULL COMMENT 'Direkter Link zum Angebot',
    article_id VARCHAR(64) NULL COMMENT 'Artikel-ID falls im HTML verfügbar',
    price_item DECIMAL(10,2) NOT NULL COMMENT 'Artikelpreis ohne Versand',
    shipping DECIMAL(10,2) NULL COMMENT 'Versandkosten (NULL wenn nicht parsebar)',
    total DECIMAL(10,2) NULL COMMENT 'Gesamtpreis (price_item + shipping)',
    currency CHAR(3) NOT NULL DEFAULT 'EUR' COMMENT 'Währung',
    quantity INT UNSIGNED NULL COMMENT 'Verfügbare Menge',
    `condition` VARCHAR(32) NULL COMMENT 'Zustand (MT, NM, EX, GD, LP, PL, PO)',
    language VARCHAR(32) NULL COMMENT 'Sprache der Karte',
    is_foil TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1=Foil, 0=Normal',
    seller_name VARCHAR(255) NULL COMMENT 'Verkäufername',
    seller_id VARCHAR(64) NULL COMMENT 'Verkäufer-ID',
    seller_country CHAR(2) NULL COMMENT 'Verkäuferland',
    seller_rating DECIMAL(5,2) NULL COMMENT 'Verkäuferbewertung in Prozent',
    seller_sales INT UNSIGNED NULL COMMENT 'Anzahl Verkäufe des Verkäufers',
    flags_json JSON NULL COMMENT 'Zusätzliche Flags (professional, powerseller, etc.)',
    PRIMARY KEY (id),
    INDEX idx_offer_scan_id (scan_id),
    INDEX idx_offer_article_id (article_id),
    INDEX idx_offer_seller_id (seller_id),
    INDEX idx_offer_total (total),
    INDEX idx_offer_price_item (price_item),
    CONSTRAINT fk_offer_scan FOREIGN KEY (scan_id)
        REFERENCES scan_run(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Einzelne Angebote pro Scan-Durchlauf';


-- ============================================
-- TABELLE: scan_agg
-- ============================================
-- Aggregierte Kennzahlen je scan_run (Quantile, Median, etc.)

CREATE TABLE IF NOT EXISTS scan_agg (
    scan_id BIGINT UNSIGNED NOT NULL COMMENT 'FK -> scan_run.id (1:1)',
    offer_count INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Anzahl gespeicherter Angebote',
    seller_count INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Anzahl eindeutiger Verkäufer',
    min_total DECIMAL(10,2) NULL COMMENT 'Minimum (total oder price_item)',
    p10_total DECIMAL(10,2) NULL COMMENT '10. Perzentil',
    p25_total DECIMAL(10,2) NULL COMMENT '25. Perzentil (Q1)',
    median_total DECIMAL(10,2) NULL COMMENT 'Median (50. Perzentil)',
    p75_total DECIMAL(10,2) NULL COMMENT '75. Perzentil (Q3)',
    p90_total DECIMAL(10,2) NULL COMMENT '90. Perzentil',
    max_total DECIMAL(10,2) NULL COMMENT 'Maximum',
    trimmed_mean_total DECIMAL(10,2) NULL COMMENT 'Getrimmter Mittelwert (10% trim)',
    iqr_total DECIMAL(10,2) NULL COMMENT 'Interquartilsabstand (Q3 - Q1)',
    stdev_total DECIMAL(10,2) NULL COMMENT 'Standardabweichung',
    -- Zusätzliche nützliche Metriken
    mean_total DECIMAL(10,2) NULL COMMENT 'Arithmetisches Mittel',
    mode_total DECIMAL(10,2) NULL COMMENT 'Häufigster Preis (Modus)',
    PRIMARY KEY (scan_id),
    CONSTRAINT fk_agg_scan FOREIGN KEY (scan_id)
        REFERENCES scan_run(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Aggregierte Statistiken pro Scan';


-- ============================================
-- TABELLE: deal_alert
-- ============================================
-- Erkannte Deals (unterdurchschnittliche Preise)

CREATE TABLE IF NOT EXISTS deal_alert (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Erkennungszeitpunkt (UTC)',
    scan_id BIGINT UNSIGNED NOT NULL COMMENT 'FK -> scan_run.id',
    article_id VARCHAR(64) NULL COMMENT 'Artikel-ID',
    article_url TEXT NULL COMMENT 'Direkter Link zum Deal',
    total DECIMAL(10,2) NOT NULL COMMENT 'Angebotspreis',
    baseline DECIMAL(10,2) NOT NULL COMMENT 'Vergleichswert (Rolling-Median)',
    discount_pct DECIMAL(6,3) NOT NULL COMMENT 'Rabatt in Dezimal (z.B. -0.175 = -17.5%)',
    reason VARCHAR(255) NOT NULL COMMENT 'Erkennungsgrund',
    -- Denormalisierte Felder für schnelle Abfragen
    card_name VARCHAR(255) NULL COMMENT 'Kartenname (denormalisiert)',
    set_code VARCHAR(32) NULL COMMENT 'Set-Code (denormalisiert)',
    karten_nummer VARCHAR(64) NULL COMMENT 'Kartennummer (denormalisiert)',
    land CHAR(2) NULL COMMENT 'Land (denormalisiert)',
    foil TINYINT(1) NULL COMMENT 'Foil-Status (denormalisiert)',
    seller_name VARCHAR(255) NULL COMMENT 'Verkäufer (denormalisiert)',
    `condition` VARCHAR(32) NULL COMMENT 'Zustand (denormalisiert)',
    meta_json JSON NULL COMMENT 'Zusätzliche Metadaten',
    PRIMARY KEY (id),
    INDEX idx_deal_ts (ts),
    INDEX idx_deal_card_ts (karten_nummer, set_code, land, foil, ts),
    INDEX idx_deal_scan_id (scan_id),
    CONSTRAINT fk_deal_scan FOREIGN KEY (scan_id)
        REFERENCES scan_run(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Erkannte Deal-Alarme';


-- ============================================
-- TABELLE: watchlist (falls noch nicht vorhanden)
-- ============================================
-- Karten-Watchlist für automatisches Scanning

CREATE TABLE IF NOT EXISTS watchlist (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    karten_nummer VARCHAR(64) NOT NULL COMMENT 'Kartennummer im Set',
    set_code VARCHAR(32) NOT NULL COMMENT 'Set-Kürzel',
    land CHAR(2) NOT NULL DEFAULT 'DE' COMMENT 'Länder-Filter',
    foil TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1=Foil, 0=Normal',
    aktiv TINYINT(1) NOT NULL DEFAULT 1 COMMENT '1=Aktiv, 0=Pausiert',
    erstellt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    aktualisiert DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_watchlist_card (karten_nummer, set_code, land, foil)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Watchlist für automatisches Scanning';


-- ============================================
-- TABELLE: preis_historie (Legacy-Kompatibilität)
-- ============================================
-- Alte Tabelle für Rückwärtskompatibilität

CREATE TABLE IF NOT EXISTS preis_historie (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    karten_name VARCHAR(255) NULL,
    karten_nummer VARCHAR(64) NULL,
    set_code VARCHAR(32) NULL,
    min_preis DECIMAL(10,2) NULL,
    avg_preis DECIMAL(10,2) NULL,
    max_preis DECIMAL(10,2) NULL,
    anzahl_angebote INT NULL,
    land CHAR(2) NULL,
    foil TINYINT(1) DEFAULT 0,
    zeitstempel DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_preis_historie_ts (zeitstempel),
    INDEX idx_preis_historie_card (karten_nummer, set_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Legacy-Preishistorie (Kompatibilität mit v1)';


-- ============================================
-- VIEWS für einfache Abfragen
-- ============================================

-- View: Aktuelle Deals mit allen Details
CREATE OR REPLACE VIEW v_active_deals AS
SELECT
    d.id AS deal_id,
    d.ts AS deal_time,
    d.card_name,
    d.set_code,
    d.karten_nummer,
    d.land,
    d.foil,
    d.total AS deal_price,
    d.baseline,
    ROUND(d.discount_pct * 100, 1) AS discount_percent,
    d.seller_name,
    d.`condition`,
    d.article_url,
    d.reason
FROM deal_alert d
WHERE d.ts >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
ORDER BY d.ts DESC;


-- View: Letzte Scan-Statistiken pro Karte
CREATE OR REPLACE VIEW v_latest_scan_stats AS
SELECT
    sr.id AS scan_id,
    sr.ts,
    sr.card_name,
    sr.set_code,
    sr.karten_nummer,
    sr.land,
    sr.foil,
    sa.offer_count,
    sa.seller_count,
    sa.min_total,
    sa.median_total,
    sa.max_total,
    sa.p25_total,
    sa.p75_total,
    sa.iqr_total,
    sa.stdev_total
FROM scan_run sr
INNER JOIN scan_agg sa ON sr.id = sa.scan_id
WHERE sr.ok = 1
AND sr.ts = (
    SELECT MAX(sr2.ts)
    FROM scan_run sr2
    WHERE sr2.karten_nummer = sr.karten_nummer
    AND sr2.set_code = sr.set_code
    AND sr2.land = sr.land
    AND sr2.foil = sr.foil
    AND sr2.ok = 1
);


-- View: Preisentwicklung (für Charts)
CREATE OR REPLACE VIEW v_price_history AS
SELECT
    sr.ts,
    sr.card_name,
    sr.set_code,
    sr.karten_nummer,
    sr.land,
    sr.foil,
    sa.min_total,
    sa.p10_total,
    sa.p25_total,
    sa.median_total,
    sa.p75_total,
    sa.p90_total,
    sa.max_total,
    sa.offer_count,
    sa.seller_count,
    sa.iqr_total,
    sa.trimmed_mean_total
FROM scan_run sr
INNER JOIN scan_agg sa ON sr.id = sa.scan_id
WHERE sr.ok = 1
ORDER BY sr.karten_nummer, sr.set_code, sr.land, sr.foil, sr.ts;


-- ============================================
-- STORED PROCEDURES
-- ============================================

-- Prozedur: Rolling-Median für Deal-Detection berechnen
DELIMITER //

CREATE PROCEDURE IF NOT EXISTS sp_get_rolling_baseline(
    IN p_karten_nummer VARCHAR(64),
    IN p_set_code VARCHAR(32),
    IN p_land CHAR(2),
    IN p_foil TINYINT(1),
    IN p_window_scans INT,
    OUT p_baseline DECIMAL(10,2)
)
BEGIN
    -- Berechnet den Median der letzten N Scans
    SELECT AVG(median_total) INTO p_baseline
    FROM (
        SELECT sa.median_total
        FROM scan_run sr
        INNER JOIN scan_agg sa ON sr.id = sa.scan_id
        WHERE sr.karten_nummer = p_karten_nummer
        AND sr.set_code = p_set_code
        AND sr.land = p_land
        AND sr.foil = p_foil
        AND sr.ok = 1
        AND sa.median_total IS NOT NULL
        ORDER BY sr.ts DESC
        LIMIT p_window_scans
    ) AS recent_scans;
END //

DELIMITER ;


-- ============================================
-- CLEANUP / MAINTENANCE
-- ============================================

-- Optimierung: Partitionierung für große Tabellen (optional)
-- Bei sehr großen Datenmengen kann die offer_snapshot Tabelle
-- nach Monat partitioniert werden:
--
-- ALTER TABLE offer_snapshot
-- PARTITION BY RANGE (YEAR(scan_id) * 100 + MONTH(scan_id)) (
--     PARTITION p202501 VALUES LESS THAN (202502),
--     PARTITION p202502 VALUES LESS THAN (202503),
--     ...
-- );

-- ============================================
-- GRANTS (anpassen nach Bedarf)
-- ============================================

-- GRANT SELECT, INSERT, UPDATE, DELETE ON cardmarket.scan_run TO 'cardmarket'@'localhost';
-- GRANT SELECT, INSERT, UPDATE, DELETE ON cardmarket.offer_snapshot TO 'cardmarket'@'localhost';
-- GRANT SELECT, INSERT, UPDATE, DELETE ON cardmarket.scan_agg TO 'cardmarket'@'localhost';
-- GRANT SELECT, INSERT, UPDATE, DELETE ON cardmarket.deal_alert TO 'cardmarket'@'localhost';
-- GRANT SELECT, INSERT, UPDATE, DELETE ON cardmarket.watchlist TO 'cardmarket'@'localhost';
-- GRANT EXECUTE ON PROCEDURE cardmarket.sp_get_rolling_baseline TO 'cardmarket'@'localhost';
-- FLUSH PRIVILEGES;
