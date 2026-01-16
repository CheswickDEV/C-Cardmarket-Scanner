#!/usr/bin/env python3
"""
Cardmarket Scanner - Data Retention & Pruning
==============================================
Bereinigt alte Daten basierend auf konfigurierbaren Aufbewahrungsrichtlinien.

Features:
- Löscht alte offer_snapshot Einträge (detaillierte Daten)
- Behält scan_agg Aggregationen länger
- Archiviert oder löscht alte Deal-Alerts
- Optimiert Tabellen nach dem Löschen

Verwendung:
    python3 retention.py              # Dry-Run (zeigt was gelöscht würde)
    python3 retention.py --execute    # Führt Löschungen aus
    python3 retention.py --stats      # Zeigt Statistiken

Cronjob (täglich um 3:00 Uhr):
    0 3 * * * cd /opt/cardmarket-scanner && /opt/cardmarket-scanner/venv/bin/python retention.py --execute >> /var/log/cardmarket-retention.log 2>&1
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, Tuple

import mysql.connector
from mysql.connector import pooling

# ============================================
# KONFIGURATION (Umgebungsvariablen)
# ============================================

# Datenbank
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_USER = os.getenv('DB_USER', 'cardmarket')
DB_PASS = os.getenv('DB_PASS', 'DEIN_DATENBANK_PASSWORT')
DB_NAME = os.getenv('DB_NAME', 'cardmarket')

# Aufbewahrungsfristen (in Tagen)
# Detaillierte Angebotsdaten (offer_snapshot)
RETENTION_OFFERS_DAYS = int(os.getenv('RETENTION_OFFERS_DAYS', '30'))

# Aggregierte Statistiken (scan_agg + scan_run)
RETENTION_AGGREGATES_DAYS = int(os.getenv('RETENTION_AGGREGATES_DAYS', '365'))

# Deal-Alerts
RETENTION_DEALS_DAYS = int(os.getenv('RETENTION_DEALS_DAYS', '90'))

# Legacy preis_historie
RETENTION_LEGACY_DAYS = int(os.getenv('RETENTION_LEGACY_DAYS', '365'))

# Batch-Größe für Löschoperationen
DELETE_BATCH_SIZE = int(os.getenv('DELETE_BATCH_SIZE', '10000'))

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# ============================================
# LOGGING SETUP
# ============================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================
# DATENBANK
# ============================================

def get_db_connection():
    """Erstellt eine Datenbankverbindung."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset='utf8mb4',
        autocommit=False
    )


# ============================================
# STATISTIKEN
# ============================================

def get_table_stats(conn) -> Dict[str, Dict]:
    """Sammelt Statistiken über alle Tabellen."""
    cursor = conn.cursor(dictionary=True)
    stats = {}

    tables = [
        ('scan_run', 'ts'),
        ('offer_snapshot', None),  # Keine eigene Zeitspalte, über scan_run
        ('scan_agg', None),  # Über scan_run
        ('deal_alert', 'ts'),
        ('preis_historie', 'zeitstempel'),
        ('watchlist', 'erstellt'),
    ]

    for table, ts_col in tables:
        try:
            # Zeilenanzahl
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {table}")
            row_count = cursor.fetchone()['cnt']

            # Tabellengröße
            cursor.execute("""
                SELECT
                    ROUND(data_length / 1024 / 1024, 2) as data_mb,
                    ROUND(index_length / 1024 / 1024, 2) as index_mb
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (DB_NAME, table))
            size_info = cursor.fetchone()

            # Ältester/Neuester Eintrag
            oldest = None
            newest = None
            if ts_col:
                cursor.execute(f"SELECT MIN({ts_col}) as oldest, MAX({ts_col}) as newest FROM {table}")
                time_info = cursor.fetchone()
                oldest = time_info['oldest']
                newest = time_info['newest']

            stats[table] = {
                'rows': row_count,
                'data_mb': size_info['data_mb'] if size_info else 0,
                'index_mb': size_info['index_mb'] if size_info else 0,
                'oldest': oldest,
                'newest': newest
            }

        except mysql.connector.Error as e:
            logger.warning(f"Konnte Stats für {table} nicht abrufen: {e}")
            stats[table] = {'rows': 0, 'data_mb': 0, 'index_mb': 0, 'oldest': None, 'newest': None}

    return stats


def print_stats(stats: Dict[str, Dict]):
    """Gibt Tabellenstatistiken aus."""
    print("\n" + "=" * 80)
    print("DATENBANK-STATISTIKEN")
    print("=" * 80)

    total_rows = 0
    total_size = 0

    print(f"\n{'Tabelle':<20} {'Zeilen':>12} {'Daten MB':>10} {'Index MB':>10} {'Ältester':>20}")
    print("-" * 80)

    for table, info in stats.items():
        oldest_str = info['oldest'].strftime('%Y-%m-%d') if info['oldest'] else '-'
        print(f"{table:<20} {info['rows']:>12,} {info['data_mb']:>10.2f} {info['index_mb']:>10.2f} {oldest_str:>20}")
        total_rows += info['rows']
        total_size += (info['data_mb'] or 0) + (info['index_mb'] or 0)

    print("-" * 80)
    print(f"{'GESAMT':<20} {total_rows:>12,} {total_size:>21.2f} MB")


# ============================================
# RETENTION-LOGIK
# ============================================

def count_deletable_offers(conn, cutoff_date: datetime) -> int:
    """Zählt zu löschende offer_snapshot Einträge."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM offer_snapshot os
        INNER JOIN scan_run sr ON os.scan_id = sr.id
        WHERE sr.ts < %s
    """, (cutoff_date,))
    return cursor.fetchone()[0]


def count_deletable_scans(conn, cutoff_date: datetime) -> Tuple[int, int]:
    """Zählt zu löschende scan_run und scan_agg Einträge."""
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM scan_run WHERE ts < %s", (cutoff_date,))
    scan_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*)
        FROM scan_agg sa
        INNER JOIN scan_run sr ON sa.scan_id = sr.id
        WHERE sr.ts < %s
    """, (cutoff_date,))
    agg_count = cursor.fetchone()[0]

    return scan_count, agg_count


def count_deletable_deals(conn, cutoff_date: datetime) -> int:
    """Zählt zu löschende deal_alert Einträge."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM deal_alert WHERE ts < %s", (cutoff_date,))
    return cursor.fetchone()[0]


def count_deletable_legacy(conn, cutoff_date: datetime) -> int:
    """Zählt zu löschende preis_historie Einträge."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM preis_historie WHERE zeitstempel < %s", (cutoff_date,))
    return cursor.fetchone()[0]


def delete_old_offers(conn, cutoff_date: datetime, dry_run: bool = True) -> int:
    """Löscht alte offer_snapshot Einträge in Batches."""
    if dry_run:
        return count_deletable_offers(conn, cutoff_date)

    cursor = conn.cursor()
    total_deleted = 0

    while True:
        # Batch von scan_ids finden
        cursor.execute("""
            SELECT DISTINCT os.scan_id
            FROM offer_snapshot os
            INNER JOIN scan_run sr ON os.scan_id = sr.id
            WHERE sr.ts < %s
            LIMIT %s
        """, (cutoff_date, DELETE_BATCH_SIZE // 10))

        scan_ids = [row[0] for row in cursor.fetchall()]

        if not scan_ids:
            break

        # Offers für diese scan_ids löschen
        placeholders = ','.join(['%s'] * len(scan_ids))
        cursor.execute(f"DELETE FROM offer_snapshot WHERE scan_id IN ({placeholders})", scan_ids)
        deleted = cursor.rowcount
        conn.commit()

        total_deleted += deleted
        logger.info(f"  Gelöscht: {deleted} offer_snapshot Einträge (Gesamt: {total_deleted})")

        if deleted < DELETE_BATCH_SIZE:
            break

    return total_deleted


def delete_old_scans(conn, cutoff_date: datetime, dry_run: bool = True) -> Tuple[int, int]:
    """Löscht alte scan_run und scan_agg Einträge."""
    if dry_run:
        return count_deletable_scans(conn, cutoff_date)

    cursor = conn.cursor()
    total_scans = 0
    total_aggs = 0

    while True:
        # scan_ids zum Löschen finden
        cursor.execute("""
            SELECT id FROM scan_run
            WHERE ts < %s
            LIMIT %s
        """, (cutoff_date, DELETE_BATCH_SIZE))

        scan_ids = [row[0] for row in cursor.fetchall()]

        if not scan_ids:
            break

        placeholders = ','.join(['%s'] * len(scan_ids))

        # Erst scan_agg löschen (FK)
        cursor.execute(f"DELETE FROM scan_agg WHERE scan_id IN ({placeholders})", scan_ids)
        aggs_deleted = cursor.rowcount

        # Dann scan_run
        cursor.execute(f"DELETE FROM scan_run WHERE id IN ({placeholders})", scan_ids)
        scans_deleted = cursor.rowcount

        conn.commit()

        total_scans += scans_deleted
        total_aggs += aggs_deleted

        logger.info(f"  Gelöscht: {scans_deleted} scan_run, {aggs_deleted} scan_agg (Gesamt: {total_scans}, {total_aggs})")

        if scans_deleted < DELETE_BATCH_SIZE:
            break

    return total_scans, total_aggs


def delete_old_deals(conn, cutoff_date: datetime, dry_run: bool = True) -> int:
    """Löscht alte deal_alert Einträge."""
    if dry_run:
        return count_deletable_deals(conn, cutoff_date)

    cursor = conn.cursor()
    total_deleted = 0

    while True:
        cursor.execute("""
            DELETE FROM deal_alert
            WHERE ts < %s
            LIMIT %s
        """, (cutoff_date, DELETE_BATCH_SIZE))

        deleted = cursor.rowcount
        conn.commit()

        total_deleted += deleted

        if deleted > 0:
            logger.info(f"  Gelöscht: {deleted} deal_alert Einträge (Gesamt: {total_deleted})")

        if deleted < DELETE_BATCH_SIZE:
            break

    return total_deleted


def delete_old_legacy(conn, cutoff_date: datetime, dry_run: bool = True) -> int:
    """Löscht alte preis_historie Einträge."""
    if dry_run:
        return count_deletable_legacy(conn, cutoff_date)

    cursor = conn.cursor()
    total_deleted = 0

    while True:
        cursor.execute("""
            DELETE FROM preis_historie
            WHERE zeitstempel < %s
            LIMIT %s
        """, (cutoff_date, DELETE_BATCH_SIZE))

        deleted = cursor.rowcount
        conn.commit()

        total_deleted += deleted

        if deleted > 0:
            logger.info(f"  Gelöscht: {deleted} preis_historie Einträge (Gesamt: {total_deleted})")

        if deleted < DELETE_BATCH_SIZE:
            break

    return total_deleted


def optimize_tables(conn):
    """Optimiert Tabellen nach dem Löschen."""
    cursor = conn.cursor()
    tables = ['offer_snapshot', 'scan_run', 'scan_agg', 'deal_alert', 'preis_historie']

    for table in tables:
        try:
            logger.info(f"Optimiere {table}...")
            cursor.execute(f"OPTIMIZE TABLE {table}")
            result = cursor.fetchall()
            logger.info(f"  {table}: {result}")
        except mysql.connector.Error as e:
            logger.warning(f"  Optimierung von {table} fehlgeschlagen: {e}")


# ============================================
# HAUPTLOGIK
# ============================================

def run_retention(execute: bool = False):
    """Führt die Retention-Logik aus."""
    now = datetime.now()

    cutoff_offers = now - timedelta(days=RETENTION_OFFERS_DAYS)
    cutoff_aggregates = now - timedelta(days=RETENTION_AGGREGATES_DAYS)
    cutoff_deals = now - timedelta(days=RETENTION_DEALS_DAYS)
    cutoff_legacy = now - timedelta(days=RETENTION_LEGACY_DAYS)

    mode = "AUSFÜHRUNG" if execute else "DRY-RUN (keine Änderungen)"

    logger.info("=" * 60)
    logger.info(f"Cardmarket Scanner - Data Retention [{mode}]")
    logger.info("=" * 60)
    logger.info(f"Zeitpunkt: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    logger.info("Aufbewahrungsrichtlinien:")
    logger.info(f"  offer_snapshot:  {RETENTION_OFFERS_DAYS} Tage (vor {cutoff_offers.strftime('%Y-%m-%d')})")
    logger.info(f"  scan_run/agg:    {RETENTION_AGGREGATES_DAYS} Tage (vor {cutoff_aggregates.strftime('%Y-%m-%d')})")
    logger.info(f"  deal_alert:      {RETENTION_DEALS_DAYS} Tage (vor {cutoff_deals.strftime('%Y-%m-%d')})")
    logger.info(f"  preis_historie:  {RETENTION_LEGACY_DAYS} Tage (vor {cutoff_legacy.strftime('%Y-%m-%d')})")
    logger.info("")

    conn = get_db_connection()

    try:
        # 1. offer_snapshot bereinigen
        logger.info("1. offer_snapshot bereinigen...")
        offers_count = delete_old_offers(conn, cutoff_offers, dry_run=not execute)
        logger.info(f"   {'Würde löschen' if not execute else 'Gelöscht'}: {offers_count:,} Einträge")

        # 2. scan_run und scan_agg bereinigen
        logger.info("2. scan_run und scan_agg bereinigen...")
        scans_count, aggs_count = delete_old_scans(conn, cutoff_aggregates, dry_run=not execute)
        logger.info(f"   {'Würde löschen' if not execute else 'Gelöscht'}: {scans_count:,} scan_run, {aggs_count:,} scan_agg")

        # 3. deal_alert bereinigen
        logger.info("3. deal_alert bereinigen...")
        deals_count = delete_old_deals(conn, cutoff_deals, dry_run=not execute)
        logger.info(f"   {'Würde löschen' if not execute else 'Gelöscht'}: {deals_count:,} Einträge")

        # 4. preis_historie bereinigen
        logger.info("4. preis_historie bereinigen...")
        legacy_count = delete_old_legacy(conn, cutoff_legacy, dry_run=not execute)
        logger.info(f"   {'Würde löschen' if not execute else 'Gelöscht'}: {legacy_count:,} Einträge")

        # 5. Tabellen optimieren (nur bei Ausführung)
        if execute and (offers_count > 0 or scans_count > 0 or deals_count > 0 or legacy_count > 0):
            logger.info("")
            logger.info("5. Tabellen optimieren...")
            optimize_tables(conn)

        logger.info("")
        logger.info("=" * 60)
        logger.info("Retention abgeschlossen")
        logger.info("=" * 60)

    finally:
        conn.close()


def main():
    """Hauptfunktion."""
    parser = argparse.ArgumentParser(
        description='Cardmarket Scanner - Data Retention & Pruning',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Umgebungsvariablen:
  RETENTION_OFFERS_DAYS      Aufbewahrung für offer_snapshot (default: 30)
  RETENTION_AGGREGATES_DAYS  Aufbewahrung für scan_run/agg (default: 365)
  RETENTION_DEALS_DAYS       Aufbewahrung für deal_alert (default: 90)
  RETENTION_LEGACY_DAYS      Aufbewahrung für preis_historie (default: 365)
  DELETE_BATCH_SIZE          Batch-Größe für Löschungen (default: 10000)

Beispiele:
  python retention.py              # Dry-Run
  python retention.py --execute    # Ausführen
  python retention.py --stats      # Nur Statistiken anzeigen
        """
    )

    parser.add_argument(
        '--execute', '-e',
        action='store_true',
        help='Führt die Löschungen tatsächlich aus (ohne: Dry-Run)'
    )

    parser.add_argument(
        '--stats', '-s',
        action='store_true',
        help='Zeigt nur Statistiken an, keine Retention'
    )

    parser.add_argument(
        '--optimize', '-o',
        action='store_true',
        help='Optimiert alle Tabellen (ohne Löschungen)'
    )

    args = parser.parse_args()

    conn = get_db_connection()

    try:
        if args.stats:
            stats = get_table_stats(conn)
            print_stats(stats)

        elif args.optimize:
            logger.info("Optimiere Tabellen...")
            optimize_tables(conn)
            logger.info("Fertig.")

        else:
            run_retention(execute=args.execute)

            # Statistiken nach Ausführung anzeigen
            if args.execute:
                print("\n")
                stats = get_table_stats(conn)
                print_stats(stats)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
