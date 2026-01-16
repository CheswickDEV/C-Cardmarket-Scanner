#!/usr/bin/env python3
"""
Unit Tests für den Cardmarket HTML Parser
=========================================
Testet die Parser-Funktionen mit gespeicherten HTML-Fixtures.

Ausführen:
    python -m pytest tests/test_parser.py -v
    python -m pytest tests/test_parser.py -v --tb=short
"""

import os
import sys
from decimal import Decimal
from pathlib import Path

import pytest

# Projektverzeichnis zum Path hinzufügen
sys.path.insert(0, str(Path(__file__).parent.parent))

from cron_scanner_v2 import (
    CardmarketParser,
    Offer,
    AggregatedStats,
    calculate_aggregates,
    parse_price,
    parse_rating,
    parse_int,
    normalize_condition,
    condition_meets_minimum,
    calculate_percentile,
    calculate_trimmed_mean,
    get_card_name,
    generate_cardmarket_url,
)


# ============================================
# FIXTURES
# ============================================

FIXTURES_DIR = Path(__file__).parent / 'fixtures'


@pytest.fixture
def sample_html():
    """Lädt die Beispiel-Produktseite."""
    with open(FIXTURES_DIR / 'sample_product_page.html', 'r', encoding='utf-8') as f:
        return f.read()


@pytest.fixture
def no_offers_html():
    """Lädt die Seite ohne Angebote."""
    with open(FIXTURES_DIR / 'no_offers_page.html', 'r', encoding='utf-8') as f:
        return f.read()


@pytest.fixture
def not_found_html():
    """Lädt die 404-Seite."""
    with open(FIXTURES_DIR / 'not_found_page.html', 'r', encoding='utf-8') as f:
        return f.read()


# ============================================
# PARSER TESTS
# ============================================

class TestCardmarketParser:
    """Tests für die CardmarketParser-Klasse."""

    def test_parse_offers_count(self, sample_html):
        """Testet dass die richtige Anzahl Angebote gefunden wird."""
        parser = CardmarketParser(sample_html)
        offers = parser.parse()

        assert len(offers) == 8

    def test_parse_offer_prices(self, sample_html):
        """Testet die Extraktion der Preise."""
        parser = CardmarketParser(sample_html)
        offers = parser.parse()

        # Erstes Angebot: 2,50€
        assert offers[0].price_item == Decimal('2.50')
        assert offers[0].shipping == Decimal('1.50')
        assert offers[0].total == Decimal('4.00')

    def test_parse_offer_conditions(self, sample_html):
        """Testet die Extraktion der Zustände."""
        parser = CardmarketParser(sample_html)
        offers = parser.parse()

        conditions = [o.condition for o in offers]
        assert 'NM' in conditions
        assert 'MT' in conditions
        assert 'EX' in conditions
        assert 'GD' in conditions
        assert 'PL' in conditions

    def test_parse_seller_info(self, sample_html):
        """Testet die Extraktion der Verkäuferinformationen."""
        parser = CardmarketParser(sample_html)
        offers = parser.parse()

        # Erstes Angebot
        assert offers[0].seller_name == 'TopSeller123'
        assert offers[0].seller_rating == 98.5

    def test_parse_quantity(self, sample_html):
        """Testet die Extraktion der Mengen."""
        parser = CardmarketParser(sample_html)
        offers = parser.parse()

        quantities = [o.quantity for o in offers if o.quantity]
        assert 3 in quantities
        assert 5 in quantities
        assert 10 in quantities

    def test_parse_foil(self, sample_html):
        """Testet die Erkennung von Foil-Karten."""
        parser = CardmarketParser(sample_html)
        offers = parser.parse()

        foil_offers = [o for o in offers if o.is_foil]
        assert len(foil_offers) >= 1

    def test_parse_article_id(self, sample_html):
        """Testet die Extraktion der Artikel-IDs."""
        parser = CardmarketParser(sample_html)
        offers = parser.parse()

        assert offers[0].article_id == '98765001'
        assert offers[1].article_id == '98765002'

    def test_parse_no_offers(self, no_offers_html):
        """Testet das Verhalten bei einer Seite ohne Angebote."""
        parser = CardmarketParser(no_offers_html)
        offers = parser.parse()

        assert len(offers) == 0

    def test_parse_positions(self, sample_html):
        """Testet die korrekte Positionszuweisung."""
        parser = CardmarketParser(sample_html)
        offers = parser.parse()

        positions = [o.position for o in offers]
        assert positions == list(range(1, len(offers) + 1))

    def test_extract_product_id(self, sample_html):
        """Testet die Extraktion der Produkt-ID."""
        parser = CardmarketParser(sample_html)
        product_id = parser.extract_product_id()

        assert product_id == '12345'


# ============================================
# HELPER FUNCTION TESTS
# ============================================

class TestParsePrice:
    """Tests für die parse_price Funktion."""

    def test_parse_euro_price(self):
        assert parse_price('2,50 €') == Decimal('2.50')
        assert parse_price('10.99 €') == Decimal('10.99')
        assert parse_price('0,99€') == Decimal('0.99')

    def test_parse_price_with_eur(self):
        assert parse_price('5.00 EUR') == Decimal('5.00')

    def test_parse_invalid_price(self):
        assert parse_price('') is None
        assert parse_price('abc') is None
        assert parse_price(None) is None

    def test_parse_price_with_spaces(self):
        assert parse_price('  3,50  €  ') == Decimal('3.50')


class TestParseRating:
    """Tests für die parse_rating Funktion."""

    def test_parse_percentage(self):
        assert parse_rating('98.5%') == 98.5
        assert parse_rating('100%') == 100.0
        assert parse_rating('95,5%') == 95.5

    def test_parse_invalid_rating(self):
        assert parse_rating('') is None
        assert parse_rating(None) is None


class TestParseInt:
    """Tests für die parse_int Funktion."""

    def test_parse_integer(self):
        assert parse_int('123') == 123
        assert parse_int('1,234') == 1234
        assert parse_int('5.678') == 5678

    def test_parse_invalid_int(self):
        assert parse_int('') is None
        assert parse_int(None) is None
        assert parse_int('abc') is None


class TestNormalizeCondition:
    """Tests für die normalize_condition Funktion."""

    def test_normalize_short_codes(self):
        assert normalize_condition('NM') == 'NM'
        assert normalize_condition('MT') == 'MT'
        assert normalize_condition('EX') == 'EX'
        assert normalize_condition('GD') == 'GD'
        assert normalize_condition('LP') == 'LP'
        assert normalize_condition('PL') == 'PL'
        assert normalize_condition('PO') == 'PO'

    def test_normalize_full_names(self):
        assert normalize_condition('Near Mint') == 'NM'
        assert normalize_condition('Mint') == 'MT'
        assert normalize_condition('Excellent') == 'EX'
        assert normalize_condition('Good') == 'GD'
        assert normalize_condition('Light Played') == 'LP'
        assert normalize_condition('Played') == 'PL'
        assert normalize_condition('Poor') == 'PO'

    def test_normalize_case_insensitive(self):
        assert normalize_condition('near mint') == 'NM'
        assert normalize_condition('MINT') == 'MT'

    def test_normalize_empty(self):
        assert normalize_condition('') is None
        assert normalize_condition(None) is None


class TestConditionMeetsMinimum:
    """Tests für die condition_meets_minimum Funktion."""

    def test_meets_minimum(self):
        assert condition_meets_minimum('NM', 'GD') is True
        assert condition_meets_minimum('MT', 'NM') is True
        assert condition_meets_minimum('EX', 'EX') is True

    def test_does_not_meet_minimum(self):
        assert condition_meets_minimum('GD', 'NM') is False
        assert condition_meets_minimum('PL', 'GD') is False
        assert condition_meets_minimum('PO', 'LP') is False

    def test_empty_condition(self):
        assert condition_meets_minimum(None, 'GD') is False
        assert condition_meets_minimum('', 'GD') is False


# ============================================
# AGGREGATION TESTS
# ============================================

class TestCalculatePercentile:
    """Tests für die calculate_percentile Funktion."""

    def test_percentiles(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]

        assert calculate_percentile(values, 0) == Decimal('1.00')
        assert calculate_percentile(values, 50) == Decimal('5.50')
        assert calculate_percentile(values, 100) == Decimal('10.00')

    def test_percentile_single_value(self):
        assert calculate_percentile([5.0], 50) == Decimal('5.00')

    def test_percentile_empty(self):
        assert calculate_percentile([], 50) is None


class TestCalculateTrimmedMean:
    """Tests für die calculate_trimmed_mean Funktion."""

    def test_trimmed_mean(self):
        # Mit Ausreißern
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 100.0]
        trimmed = calculate_trimmed_mean(values, 0.1)

        # Ohne die extremen 10% sollte der Mittelwert näher an 5 sein
        assert float(trimmed) < 15  # Viel kleiner als normaler Mittelwert

    def test_trimmed_mean_empty(self):
        assert calculate_trimmed_mean([]) is None


class TestCalculateAggregates:
    """Tests für die calculate_aggregates Funktion."""

    def test_aggregates_basic(self, sample_html):
        parser = CardmarketParser(sample_html)
        offers = parser.parse()
        stats = calculate_aggregates(offers)

        assert stats.offer_count == 8
        assert stats.seller_count > 0
        assert stats.min_total is not None
        assert stats.max_total is not None
        assert stats.median_total is not None
        assert stats.min_total <= stats.median_total <= stats.max_total

    def test_aggregates_empty(self):
        stats = calculate_aggregates([])

        assert stats.offer_count == 0
        assert stats.seller_count == 0
        assert stats.min_total is None
        assert stats.median_total is None

    def test_aggregates_iqr(self, sample_html):
        parser = CardmarketParser(sample_html)
        offers = parser.parse()
        stats = calculate_aggregates(offers)

        if stats.p25_total and stats.p75_total:
            expected_iqr = stats.p75_total - stats.p25_total
            assert stats.iqr_total == expected_iqr


# ============================================
# URL GENERATION TESTS
# ============================================

class TestUrlGeneration:
    """Tests für die URL-Generierung."""

    def test_basic_url(self):
        url = generate_cardmarket_url('OGN', 'Lee Sin Blind Monk')
        assert 'Lee-Sin-Blind-Monk' in url
        assert 'Origins' in url
        assert CARDMARKET_BASE_URL in url

    def test_url_with_country(self):
        url = generate_cardmarket_url('OGN', 'Lee Sin Blind Monk', 'DE')
        assert 'sellerCountry=7' in url

    def test_url_with_foil(self):
        url = generate_cardmarket_url('OGN', 'Lee Sin Blind Monk', is_foil=True)
        assert 'isFoil=Y' in url

    def test_url_with_country_and_foil(self):
        url = generate_cardmarket_url('OGN', 'Lee Sin Blind Monk', 'DE', True)
        assert 'sellerCountry=7' in url
        assert 'isFoil=Y' in url


# ============================================
# CARD NAME LOOKUP TESTS
# ============================================

class TestCardNameLookup:
    """Tests für die Kartennamenssuche."""

    def test_lookup_ogn_card(self):
        assert get_card_name('257', 'OGN') == 'Lee Sin Blind Monk'
        assert get_card_name('1', 'OGN') == 'Blazing Scorcher'

    def test_lookup_ogs_card(self):
        assert get_card_name('0', 'OGS') == 'Buff'
        assert get_card_name('24', 'OGS') == 'Decisive Strike'

    def test_lookup_with_leading_zeros(self):
        assert get_card_name('001', 'OGN') == 'Blazing Scorcher'

    def test_lookup_nonexistent(self):
        assert get_card_name('999', 'OGN') is None
        assert get_card_name('1', 'INVALID') is None


# ============================================
# OFFER DATACLASS TESTS
# ============================================

class TestOfferDataclass:
    """Tests für die Offer-Datenklasse."""

    def test_total_calculation(self):
        offer = Offer(
            position=1,
            price_item=Decimal('2.50'),
            shipping=Decimal('1.50')
        )
        assert offer.total == Decimal('4.00')

    def test_total_without_shipping(self):
        offer = Offer(
            position=1,
            price_item=Decimal('2.50')
        )
        assert offer.total == Decimal('2.50')


# ============================================
# INTEGRATION TESTS
# ============================================

class TestIntegration:
    """Integrationstests für den gesamten Parser-Flow."""

    def test_full_parsing_flow(self, sample_html):
        """Testet den kompletten Parsing-Ablauf."""
        # Parsen
        parser = CardmarketParser(sample_html)
        offers = parser.parse(max_offers=150)

        # Validierung der Offers
        assert len(offers) > 0

        for offer in offers:
            assert offer.price_item is not None
            assert offer.price_item > Decimal('0')
            assert offer.position > 0
            assert offer.currency == 'EUR'

        # Aggregation
        stats = calculate_aggregates(offers)

        assert stats.offer_count == len(offers)
        assert stats.min_total <= stats.max_total

        # Perzentile sollten geordnet sein
        if all([stats.p10_total, stats.p25_total, stats.median_total,
                stats.p75_total, stats.p90_total]):
            assert stats.p10_total <= stats.p25_total
            assert stats.p25_total <= stats.median_total
            assert stats.median_total <= stats.p75_total
            assert stats.p75_total <= stats.p90_total

    def test_deal_detection_scenario(self, sample_html):
        """Testet ein Deal-Detection-Szenario."""
        parser = CardmarketParser(sample_html)
        offers = parser.parse()
        stats = calculate_aggregates(offers)

        # Simulierte Baseline (Median der letzten Scans)
        baseline = stats.median_total
        threshold = 0.15  # 15%

        if baseline:
            deal_price = float(baseline) * (1 - threshold)

            # Finde Angebote unter der Deal-Schwelle
            deals = [
                o for o in offers
                if o.total and float(o.total) <= deal_price
            ]

            # Es sollte mindestens ein günstiges Angebot geben
            # (0,99€ GD oder 0,50€ PL)
            cheap_offers = [o for o in offers if o.total and float(o.total) < 3.0]
            assert len(cheap_offers) > 0


# ============================================
# MAIN
# ============================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
