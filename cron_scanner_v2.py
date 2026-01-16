#!/usr/bin/env python3
"""
Cardmarket Cron Scanner v2
==========================
Erweiterter Preis-Scanner mit strukturiertem HTML-Parsing,
detaillierter Angebotsspeicherung und Deal-Erkennung.

Features:
- Strukturiertes DOM-Parsing statt Regex
- Einzelne Angebote in offer_snapshot speichern
- Aggregierte Statistiken (Quantile, Median, IQR)
- Automatische Deal-Erkennung mit Rolling-Baseline
- Robustes Error-Handling pro Karte
- Konfiguration über Umgebungsvariablen

Verwendung:
    python3 cron_scanner_v2.py          # Vollständiger Scan
    python3 cron_scanner_v2.py scan     # Vollständiger Scan
    python3 cron_scanner_v2.py test 12 OGN DE  # Einzeltest

Cronjob (stündlich):
    0 * * * * cd /opt/cardmarket-scanner && /opt/cardmarket-scanner/venv/bin/python cron_scanner_v2.py >> /var/log/cardmarket-scanner.log 2>&1
"""

import os
import sys
import json
import time
import logging
import re
import statistics
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field, asdict
from collections import Counter

import requests
import mysql.connector
from mysql.connector import pooling
from bs4 import BeautifulSoup

# ============================================
# VERSION
# ============================================
PARSE_VERSION = "v2.0"

# ============================================
# KONFIGURATION (Umgebungsvariablen)
# ============================================

# Datenbank
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_USER = os.getenv('DB_USER', 'cardmarket')
DB_PASS = os.getenv('DB_PASS', 'DEIN_DATENBANK_PASSWORT')
DB_NAME = os.getenv('DB_NAME', 'cardmarket')

# FlareSolverr
FLARESOLVERR_URL = os.getenv('FLARESOLVERR_URL', 'http://localhost:8191/v1')

# Limits
MAX_OFFERS_PER_SCAN = int(os.getenv('MAX_OFFERS_PER_SCAN', '150'))
SLEEP_BETWEEN_CARDS_SEC = float(os.getenv('SLEEP_BETWEEN_CARDS_SEC', '2'))
REQUEST_TIMEOUT_SEC = int(os.getenv('REQUEST_TIMEOUT_SEC', '120'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))

# Deal Detection
DEAL_THRESHOLD = float(os.getenv('DEAL_THRESHOLD', '0.15'))  # 15% unter Baseline
BASELINE_WINDOW_SCANS = int(os.getenv('BASELINE_WINDOW_SCANS', '48'))
MIN_SELLER_RATING = float(os.getenv('MIN_SELLER_RATING', '90.0'))  # Mindestens 90%
MIN_CONDITION = os.getenv('MIN_CONDITION', 'GD')  # Mindestens Good

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', '')

# ============================================
# LOGGING SETUP
# ============================================

def setup_logging():
    """Konfiguriert das Logging."""
    handlers = [logging.StreamHandler()]

    if LOG_FILE:
        handlers.append(logging.FileHandler(LOG_FILE))

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=handlers
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# ============================================
# KONSTANTEN
# ============================================

SESSION_ID = "cardmarket_cron_v2_session"

CARDMARKET_BASE_URL = "https://www.cardmarket.com/de/Riftbound/Products/Singles"

# Länder-Codes für Cardmarket URL-Parameter
COUNTRY_CODES = {
    'AT': 1, 'BE': 2, 'CH': 4, 'DE': 7, 'DK': 8,
    'ES': 10, 'FR': 12, 'UK': 13, 'IT': 17, 'NL': 18,
    'PL': 19, 'PT': 20, 'SE': 22, 'CZ': 6, 'FI': 11,
}

# Set-Namen für URL
SET_URL_MAP = {
    'OGN': 'Origins',
    'OGS': 'Proving-Grounds',
    'OGNX': 'Origins-Promos'
}

# Condition-Ranking (höher = besser)
CONDITION_RANK = {
    'MT': 7, 'M': 7, 'MINT': 7,
    'NM': 6, 'NEAR MINT': 6,
    'EX': 5, 'EXCELLENT': 5,
    'GD': 4, 'GOOD': 4,
    'LP': 3, 'LIGHT PLAYED': 3, 'LIGHTLY PLAYED': 3,
    'PL': 2, 'PLAYED': 2,
    'PO': 1, 'POOR': 1,
}

# Karten-Mapping (Nummer -> Name)
CARD_MAP = {
    'OGS': {
        '0': 'Buff', '1': 'Annie Fiery', '2': 'Firestorm', '3': 'Incinerate', '4': 'Master Yi Meditative',
        '5': 'Zephyr Sage', '6': 'Lux Illuminated', '7': 'Garen Rugged', '8': "Gentlemen's Duel", '9': 'Master Yi Honed',
        '10': 'Annie Stubborn', '11': 'Flash', '12': 'Blast of Power', '13': 'Garen Commander', '14': 'Lux Crownguard',
        '15': 'Recruit the Vanguard', '16': 'Vanguard Attendant', '17': 'Annie Dark Child', '18': 'Tibbers',
        '19': 'Master Yi Wuju Bladesman', '20': 'Highlander', '21': 'Lux Lady of Luminosity', '22': 'Final Spark',
        '23': 'Garen Might of Demacia', '24': 'Decisive Strike'
    },
    'OGN': {
        '1': 'Blazing Scorcher', '2': 'Brazen Buccaneer', '3': 'Chemtech Enforcer', '4': 'Cleave', '5': 'Disintegrate',
        '6': 'Flame Chompers', '7': 'Fury Rune V1 Common', '7a': 'Fury Rune V2 Showcase', '8': 'Get Excited!', '9': 'Hextech Ray',
        '10': 'Legion Rearguard', '11': 'Magma Wurm', '12': 'Noxus Hopeful', '13': 'Pouty Poro', '14': 'Sky Splitter',
        '15': 'Captain Farron', '16': 'Dangerous Duo', '17': 'Iron Ballista', '18': 'Noxus Saboteur', '19': 'Raging Soul',
        '20': 'Scrapyard Champion', '21': 'Sun Disc', '22': 'Thermo Beam', '23': 'Unlicensed Armory', '24': 'Void Seeker',
        '25': 'Blind Fury', '26': 'Brynhir Thundersong', '27': 'Darius Trifarian V1 Rare', '28': 'Draven Showboat',
        '29': 'Falling Star', '30': 'Jinx Demolitionist V1 Rare', '31': 'Raging Firebrand', '32': 'Ravenborn Tome',
        '33': 'Shakedown', '34': 'Tryndamere Barbarian', '35': 'Vayne Hunter', '36': 'Vi Destructive', '37': 'Immortal Phoenix',
        '38': 'Kadregrin the Infernal', '39': "Kai'Sa Survivor V1 Epic", '40': 'Seal of Rage', '41': 'Volibear Furious V1 Epic',
        '42': 'Calm Rune V1 Common', '42a': 'Calm Rune V2 Showcase', '43': 'Charm', '44': 'Clockwork Keeper', '45': 'Defy',
        '46': 'En Garde', '47': 'Find Your Center', '48': 'Meditation', '49': 'Playful Phantom', '50': 'Rune Prison',
        '51': 'Solari Shieldbearer', '52': 'Stalwart Poro', '53': 'Stand United', '54': 'Sunlit Guardian', '55': 'Wielder of Water',
        '56': 'Adaptatron', '57': 'Block', '58': 'Discipline', '59': 'Eclipse Herald', '60': 'Mask of Foresight',
        '61': 'Poro Herder', '62': 'Reinforce', '63': "Spirit's Refuge", '64': 'Wind Wall', '65': 'Wizened Elder',
        '66': 'Ahri Alluring V1 Rare', '67': 'Blitzcrank Impassive', '68': 'Caitlyn Patrolling', '69': 'Last Stand',
        '70': 'Mageseeker Warden', '71': 'Party Favors', '72': 'Solari Shrine', '73': 'Sona Harmonious', '74': 'Taric Protector',
        '75': 'Tasty Faefolk', '76': 'Yasuo Remorseful V1 Rare', '77': "Zhonya's Hourglass", '78': 'Lee Sin Ascetic V1 Epic',
        '79': 'Leona Zealot V1 Epic', '80': 'Mystic Reversal', '81': 'Seal of Focus', '82': 'Whiteflame Protector',
        '83': 'Consult the Past', '84': 'Eager Apprentice', '85': 'Falling Comet', '86': 'Jeweled Colossus',
        '87': 'Lecturing Yordle', '88': 'MegaMech', '89': 'Mind Rune V1 Common', '89a': 'Mind Rune V2 Showcase',
        '90': 'Orb of Regret', '91': 'Pit Crew', '92': 'Riptide Rex', '93': 'Smoke Screen', '94': 'Sprite Call',
        '95': 'Stupefy', '96': 'Watchful Sentry', '97': 'Blastcone Fae', '98': 'Energy Conduit', '99': 'Garbage Grabber',
        '100': 'Gemcraft Seer', '101': 'Mushroom Pouch', '102': 'Portal Rescue', '103': 'Ravenbloom Student', '104': 'Retreat',
        '105': 'Singularity', '106': 'Sprite Mother', '107': 'Ava Achiever', '108': 'Convergent Mutation', '109': 'Dr Mundo Expert',
        '110': 'Ekko Recurrent', '111': 'Heimerdinger Inventor', '112': "Kai'Sa Evolutionary V1 Rare", '113': 'Malzahar Fanatic',
        '114': 'Progress Day', '115': 'Promising Future', '116': 'Thousand Tailed Watcher', '117': 'Viktor Innovator V1 Rare',
        '118': 'Wraith of Echoes', '119': 'Ahri Inquisitive V1 Epic', '120': 'Seal of Insight', '121': 'Teemo Strategist V1 Epic',
        '122': 'Time Warp', '123': 'Unchecked Power', '124': 'Arena Bar', '125': 'Bilgewater Bully', '126': 'Body Rune V1 Common',
        '126a': 'Body Rune V2 Showcase', '127': 'Cannon Barrage', '128': 'Challenge', '129': 'Confront', '130': 'Crackshot Corsair',
        '131': 'Dune Drake', '132': 'First Mate', '133': 'Flurry of Blades', '134': 'Mobilize', '135': 'Pakaa Cub',
        '136': 'Pit Rookie', '137': 'Stormclaw Ursine', '138': 'Catalyst of Aeons', '139': 'Cithria of Cloudfield',
        '140': 'Herald of Scales', '141': 'Kinkou Monk', '142': 'Mountain Drake', '143': "Pirate's Haven", '144': 'Spoils of War',
        '145': 'Unyielding Spirit', '146': 'Wallop', '147': 'Wildclaw Shaman', '148': 'Anivia Primal', '149': 'Carnivorous Snapvine',
        '150': 'Kraken Hunter', '151': 'Lee Sin Centered V1 Rare', '152': 'Mistfall', '153': 'Overt Operation',
        '154': 'Primal Strength', '155': 'Qiyana Victorious', '156': 'Sabotage', '157': 'Udyr Wildman',
        '158': 'Volibear Imposing V1 Rare', '159': 'Warwick Hunter', '160': 'Dazzling Aurora', '161': 'Deadbloom Predator',
        '162': 'Miss Fortune Captain V1 Epic', '163': 'Seal of Strength', '164': 'Sett Brawler V1 Epic', '165': 'Cemetery Attendant',
        '166': 'Chaos Rune V1 Common', '166a': 'Chaos Rune V2 Showcase', '167': 'Ember Monk', '168': 'Fight or Flight',
        '169': 'Gust', '170': 'Morbid Return', '171': 'Mystic Poro', '172': 'Rebuke', '173': 'Ride the Wind', '174': 'Sai Scout',
        '175': 'Shipyard Skulker', '176': 'Sneaky Deckhand', '177': 'Stealthy Pursuer', '178': 'Undercover Agent',
        '179': 'Acceptable Losses', '180': 'Fading Memories', '181': 'Pack of Wonders', '182': 'Scrapheap', '183': 'Stacked Deck',
        '184': 'The Syren', '185': 'Traveling Merchant', '186': 'Treasure Trove', '187': 'Whirlwind', '188': 'Zaunite Bouncer',
        '189': 'Kayn Unleashed', '190': "Kog'Maw Caustic", '191': 'Maddened Marauder', '192': 'Mindsplitter',
        '193': 'Miss Fortune Buccaneer V1 Rare', '194': 'Nocturne Horrifying', '195': 'Rhasa the Sunderer', '196': 'Soulgorger',
        '197': 'Teemo Scout V1 Rare', '198': 'The Harrowing', '199': 'Tideturner', '200': 'Twisted Fate Gambler',
        '201': 'Invert Timelines', '202': 'Jinx Rebel V1 Epic', '203': 'Possession', '204': 'Seal of Discord',
        '205': 'Yasuo Windrider V1 Epic', '206': 'Back to Back', '207': 'Call to Glory', '208': 'Cruel Patron',
        '209': 'Cull the Weak', '210': 'Daring Poro', '211': 'Faithful Manufactor', '212': 'Forge of the Future',
        '213': 'Hidden Blade', '214': 'Order Rune V1 Common', '214a': 'Order Rune V2 Overnumbered', '215': 'Petty Officer',
        '216': 'Soaring Scout', '217': 'Trifarian Gloryseeker', '218': 'Vanguard Captain', '219': 'Vanguard Sergeant',
        '220': 'Facebreaker', '221': 'Imperial Decree', '222': 'Noxian Drummer', '223': 'Peak Guardian', '224': 'Salvage',
        '225': 'Solari Chief', '226': 'Spectral Matron', '227': 'Symbol of the Solari', '228': 'Vanguard Helm', '229': 'Vengeance',
        '230': 'Albus Ferros', '231': 'Commander Ledros', '232': 'Fiora Victorious', '233': 'Grand Strategem',
        '234': 'Harnessed Dragon', '235': 'Karma Channeler', '236': 'Karthus Eternal', '237': "King's Edict",
        '238': 'Leona Determined V1 Rare', '239': 'Machine Evangel', '240': 'Sett Kingpin V1 Rare', '241': 'Shen Kinkou',
        '242': 'Baited Hook', '243': 'Darius Executioner V1 Epic', '244': 'Divine Judgment', '245': 'Seal of Unity',
        '246': 'Viktor Leader V1 Epic', '247': "Kai'Sa Daughter of the Void V1 Rare", '248': 'Icathian Rain',
        '249': 'Volibear Relentless Storm V1 Rare', '250': 'Stormbringer', '251': 'Jinx Loose Cannon V1 Rare',
        '252': 'Super Mega Death Rocket', '253': 'Darius Hand of Noxus V1 Rare', '254': 'Noxian Guillotine',
        '255': 'Ahri NineTailed Fox V1 Rare', '256': 'FoxFire', '257': 'Lee Sin Blind Monk', '258': "Dragon's Rage",
        '259': 'Yasuo Unforgiven V1 Rare', '260': 'Last Breath', '261': 'Leona Radiant Dawn V1 Rare', '262': 'Zenith Blade',
        '263': 'Teemo Swift Scout V1 Rare', '264': 'Guerilla Warfare', '265': 'Viktor Herald of the Arcane',
        '266': 'Siphon Power', '267': 'Miss Fortune Bounty Hunter V1 Rare', '268': 'Bullet Time',
        '269': 'Sett The Boss V1 Rare', '270': 'Showstopper'
    },
    'OGNX': {
        '0': 'Buff', '1': 'Blazing Scorcher', '7b': 'Fury Rune', '13': 'Pouty Poro', '24': 'Void Seeker',
        '34': 'Tryndamere Barbarian', '42b': 'Calm Rune', '52': 'Stalwart Poro', '55': 'Wielder of Water',
        '58': 'Discipline', '66': 'Ahri Alluring', '67': 'Blitzcrank Impassive', '78': 'Lee Sin Ascetic',
        '83': 'Consult the Past', '89b': 'Mind Rune', '92': 'Riptide Rex', '103': 'Ravenbloom Student',
        '111': 'Heimerdinger Inventor V2 Rare', '126b': 'Body Rune', '128': 'Challenge', '135': 'Pakaa Cub',
        '137': 'Stormclaw Ursine', '166b': 'Chaos Rune', '171': 'Mystic Poro', '177': 'Stealthy Pursuer',
        '183': 'Stacked Deck', '193': 'Miss Fortune Buccaneer', '197': 'Teemo Scout V1 Rare',
        '202': 'Jinx Rebel V1 Epic', '210': 'Daring Poro', '214b': 'Order Rune', '218': 'Vanguard Captain',
        '229': 'Vengeance', '246': 'Viktor Leader V1 Epic', '247': "Kai'Sa Daughter of the Void",
        '249': 'Volibear Relentless Storm', '251': 'Jinx Loose Cannon', '253': 'Darius Hand of Noxus',
        '255': 'Ahri Nine Tailed Fox', '257': 'Lee Sin Blind Monk', '259': 'Yasuo Unforgiven',
        '261': 'Leona Radiant Dawn', '263': 'Teemo Swift Scout V1 Rare', '265': 'Viktor Herald of the Arcane',
        '267': 'Miss Fortune Bounty Hunter', '269': 'Sett The Boss'
    }
}


# ============================================
# DATENKLASSEN
# ============================================

@dataclass
class Offer:
    """Repräsentiert ein einzelnes Angebot."""
    position: int
    price_item: Decimal
    currency: str = 'EUR'
    shipping: Optional[Decimal] = None
    total: Optional[Decimal] = None
    quantity: Optional[int] = None
    condition: Optional[str] = None
    language: Optional[str] = None
    is_foil: bool = False
    seller_name: Optional[str] = None
    seller_id: Optional[str] = None
    seller_country: Optional[str] = None
    seller_rating: Optional[float] = None
    seller_sales: Optional[int] = None
    article_url: Optional[str] = None
    article_id: Optional[str] = None
    flags: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Berechnet total wenn möglich."""
        if self.total is None and self.price_item is not None:
            if self.shipping is not None:
                self.total = self.price_item + self.shipping
            else:
                self.total = self.price_item


@dataclass
class ScanResult:
    """Ergebnis eines einzelnen Scans."""
    success: bool
    url: str
    card_name: Optional[str] = None
    offers: List[Offer] = field(default_factory=list)
    http_status: Optional[int] = None
    error: Optional[str] = None
    product_id: Optional[str] = None


@dataclass
class AggregatedStats:
    """Aggregierte Statistiken für einen Scan."""
    offer_count: int = 0
    seller_count: int = 0
    min_total: Optional[Decimal] = None
    p10_total: Optional[Decimal] = None
    p25_total: Optional[Decimal] = None
    median_total: Optional[Decimal] = None
    p75_total: Optional[Decimal] = None
    p90_total: Optional[Decimal] = None
    max_total: Optional[Decimal] = None
    trimmed_mean_total: Optional[Decimal] = None
    iqr_total: Optional[Decimal] = None
    stdev_total: Optional[Decimal] = None
    mean_total: Optional[Decimal] = None
    mode_total: Optional[Decimal] = None


# ============================================
# HILFSFUNKTIONEN
# ============================================

def get_card_name(card_number: str, set_code: str) -> Optional[str]:
    """Ermittelt den Kartennamen aus der Nummer."""
    set_map = CARD_MAP.get(set_code, {})

    if card_number in set_map:
        return set_map[card_number]

    stripped = card_number.lstrip('0')
    if stripped in set_map:
        return set_map[stripped]

    return None


def generate_cardmarket_url(set_code: str, card_name: str, country_code: str = None, is_foil: bool = False) -> str:
    """Generiert die Cardmarket-URL für eine Karte mit Länderfilter."""
    set_url_name = SET_URL_MAP.get(set_code, 'Origins')

    slug = (card_name
            .replace(' - ', '-')
            .replace(' ', '-')
            .replace(',', '')
            .replace('.', '')
            .replace("'", '')
            .replace('--', '-'))

    url = f"{CARDMARKET_BASE_URL}/{set_url_name}/{slug}"

    params = []
    if country_code and country_code in COUNTRY_CODES:
        params.append(f"sellerCountry={COUNTRY_CODES[country_code]}")
    if is_foil:
        params.append("isFoil=Y")

    if params:
        url += "?" + "&".join(params)

    return url


def parse_price(price_str: str) -> Optional[Decimal]:
    """Parst einen Preisstring zu Decimal."""
    if not price_str:
        return None

    try:
        cleaned = (price_str
                   .replace('€', '')
                   .replace('EUR', '')
                   .replace(' ', '')
                   .replace(',', '.')
                   .strip())

        match = re.search(r'(\d+\.?\d*)', cleaned)
        if match:
            return Decimal(match.group(1)).quantize(Decimal('0.01'))
    except (InvalidOperation, ValueError):
        pass

    return None


def parse_rating(rating_str: str) -> Optional[float]:
    """Parst einen Rating-String zu Float (Prozent)."""
    if not rating_str:
        return None

    try:
        match = re.search(r'(\d+[.,]?\d*)', rating_str.replace(',', '.'))
        if match:
            return float(match.group(1))
    except ValueError:
        pass

    return None


def parse_int(value_str: str) -> Optional[int]:
    """Parst einen String zu Integer."""
    if not value_str:
        return None

    try:
        cleaned = re.sub(r'[^\d]', '', value_str)
        return int(cleaned) if cleaned else None
    except ValueError:
        return None


def normalize_condition(condition: str) -> Optional[str]:
    """Normalisiert Condition-Strings."""
    if not condition:
        return None

    upper = condition.upper().strip()

    # Direkte Matches
    for code in ['MT', 'NM', 'EX', 'GD', 'LP', 'PL', 'PO']:
        if code in upper:
            return code

    # Vollständige Namen
    mapping = {
        'MINT': 'MT', 'NEAR MINT': 'NM', 'EXCELLENT': 'EX',
        'GOOD': 'GD', 'LIGHT': 'LP', 'PLAYED': 'PL', 'POOR': 'PO'
    }

    for key, val in mapping.items():
        if key in upper:
            return val

    return condition[:32] if condition else None


def condition_meets_minimum(condition: Optional[str], min_condition: str) -> bool:
    """Prüft ob ein Zustand das Minimum erfüllt."""
    if not condition:
        return False

    cond_rank = CONDITION_RANK.get(condition.upper(), 0)
    min_rank = CONDITION_RANK.get(min_condition.upper(), 0)

    return cond_rank >= min_rank


def calculate_percentile(sorted_values: List[float], percentile: float) -> Optional[Decimal]:
    """Berechnet ein Perzentil aus sortierten Werten."""
    if not sorted_values:
        return None

    n = len(sorted_values)
    if n == 1:
        return Decimal(str(sorted_values[0])).quantize(Decimal('0.01'))

    k = (n - 1) * (percentile / 100.0)
    f = int(k)
    c = f + 1

    if c >= n:
        return Decimal(str(sorted_values[-1])).quantize(Decimal('0.01'))

    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)

    return Decimal(str(d0 + d1)).quantize(Decimal('0.01'))


def calculate_trimmed_mean(values: List[float], trim_pct: float = 0.1) -> Optional[Decimal]:
    """Berechnet den getrimmten Mittelwert."""
    if not values:
        return None

    n = len(values)
    if n < 3:
        return Decimal(str(statistics.mean(values))).quantize(Decimal('0.01'))

    trim_count = int(n * trim_pct)
    if trim_count == 0:
        trim_count = 1 if n > 2 else 0

    sorted_vals = sorted(values)
    trimmed = sorted_vals[trim_count:n - trim_count] if trim_count > 0 else sorted_vals

    if not trimmed:
        trimmed = sorted_vals

    return Decimal(str(statistics.mean(trimmed))).quantize(Decimal('0.01'))


def utc_now() -> datetime:
    """Gibt aktuelle UTC-Zeit zurück."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ============================================
# HTML PARSER
# ============================================

class CardmarketParser:
    """Parser für Cardmarket HTML-Seiten."""

    # CSS-Selektoren für Angebote (mit Fallbacks)
    OFFER_SELECTORS = [
        'div.article-row',
        'div[class*="article"]',
        'tr.article',
        'div.row.no-gutters.article',
        '.table-body .row',
    ]

    def __init__(self, html: str):
        self.soup = BeautifulSoup(html, 'html.parser')
        self.offers: List[Offer] = []

    def parse(self, max_offers: int = MAX_OFFERS_PER_SCAN) -> List[Offer]:
        """Parst alle Angebote aus dem HTML."""
        self.offers = []

        # Produkt-Container finden
        offer_elements = self._find_offer_elements()

        for idx, element in enumerate(offer_elements[:max_offers], start=1):
            try:
                offer = self._parse_offer_element(element, idx)
                if offer and offer.price_item is not None:
                    self.offers.append(offer)
            except Exception as e:
                logger.debug(f"Fehler beim Parsen von Angebot {idx}: {e}")
                continue

        return self.offers

    def _find_offer_elements(self) -> List:
        """Findet alle Angebots-Elemente mit Fallbacks."""
        for selector in self.OFFER_SELECTORS:
            elements = self.soup.select(selector)
            if elements:
                logger.debug(f"Gefunden {len(elements)} Elemente mit Selektor: {selector}")
                return elements

        # Fallback: Suche nach Preis-Patterns und navigiere zum Container
        price_elements = self.soup.find_all(string=re.compile(r'\d+[,\.]\d{2}\s*€'))
        if price_elements:
            containers = []
            for el in price_elements:
                parent = el.find_parent(['div', 'tr'])
                if parent and parent not in containers:
                    containers.append(parent)
            return containers

        return []

    def _parse_offer_element(self, element, position: int) -> Optional[Offer]:
        """Parst ein einzelnes Angebots-Element."""
        offer = Offer(position=position, price_item=Decimal('0'))

        # === PREIS ===
        price_item = self._extract_price(element)
        if price_item is None:
            return None
        offer.price_item = price_item

        # === VERSAND ===
        offer.shipping = self._extract_shipping(element)

        # === TOTAL ===
        if offer.shipping is not None:
            offer.total = offer.price_item + offer.shipping
        else:
            offer.total = offer.price_item

        # === WÄHRUNG ===
        offer.currency = self._extract_currency(element)

        # === MENGE ===
        offer.quantity = self._extract_quantity(element)

        # === ZUSTAND ===
        offer.condition = self._extract_condition(element)

        # === SPRACHE ===
        offer.language = self._extract_language(element)

        # === FOIL ===
        offer.is_foil = self._extract_foil(element)

        # === VERKÄUFER ===
        seller_info = self._extract_seller_info(element)
        offer.seller_name = seller_info.get('name')
        offer.seller_id = seller_info.get('id')
        offer.seller_country = seller_info.get('country')
        offer.seller_rating = seller_info.get('rating')
        offer.seller_sales = seller_info.get('sales')

        # === ARTIKEL-LINK ===
        offer.article_url, offer.article_id = self._extract_article_info(element)

        # === FLAGS ===
        offer.flags = self._extract_flags(element)

        return offer

    def _extract_price(self, element) -> Optional[Decimal]:
        """Extrahiert den Artikelpreis."""
        # Verschiedene Selektoren für Preise
        price_selectors = [
            '.price-container .font-weight-bold',
            '.price-container span',
            '[class*="price"] .font-weight-bold',
            'span.font-weight-bold',
            '.color-primary',
            '[data-original-title*="price"]',
        ]

        for selector in price_selectors:
            price_el = element.select_one(selector)
            if price_el:
                price = parse_price(price_el.get_text())
                if price and Decimal('0.01') <= price <= Decimal('10000'):
                    return price

        # Fallback: Regex im gesamten Element
        text = element.get_text()
        matches = re.findall(r'(\d+[,\.]\d{2})\s*€', text)
        if matches:
            price = parse_price(matches[0])
            if price and Decimal('0.01') <= price <= Decimal('10000'):
                return price

        return None

    def _extract_shipping(self, element) -> Optional[Decimal]:
        """Extrahiert die Versandkosten."""
        shipping_selectors = [
            '.shipping-price',
            '[class*="shipping"]',
            'span[title*="Versand"]',
            'span[title*="shipping"]',
        ]

        for selector in shipping_selectors:
            ship_el = element.select_one(selector)
            if ship_el:
                return parse_price(ship_el.get_text())

        # Suche nach "+" Versand-Pattern
        text = element.get_text()
        ship_match = re.search(r'\+\s*(\d+[,\.]\d{2})\s*€', text)
        if ship_match:
            return parse_price(ship_match.group(1))

        return None

    def _extract_currency(self, element) -> str:
        """Extrahiert die Währung."""
        text = element.get_text()
        if '€' in text or 'EUR' in text:
            return 'EUR'
        if '$' in text or 'USD' in text:
            return 'USD'
        if '£' in text or 'GBP' in text:
            return 'GBP'
        return 'EUR'

    def _extract_quantity(self, element) -> Optional[int]:
        """Extrahiert die verfügbare Menge."""
        qty_selectors = [
            '.amount-container span',
            '[class*="quantity"]',
            '[class*="amount"]',
            'input[name="amount"]',
        ]

        for selector in qty_selectors:
            qty_el = element.select_one(selector)
            if qty_el:
                val = qty_el.get('value') or qty_el.get_text()
                qty = parse_int(val)
                if qty:
                    return qty

        return None

    def _extract_condition(self, element) -> Optional[str]:
        """Extrahiert den Kartenzustand."""
        cond_selectors = [
            'a[data-original-title]',
            'span[data-original-title]',
            '.product-attributes span',
            '[class*="condition"]',
        ]

        conditions = ['Mint', 'Near Mint', 'NM', 'Excellent', 'EX',
                      'Good', 'GD', 'Light Played', 'LP', 'Played', 'PL', 'Poor', 'PO']

        for selector in cond_selectors:
            for el in element.select(selector):
                title = el.get('data-original-title', '') or el.get('title', '')
                text = el.get_text()

                for cond in conditions:
                    if cond.lower() in title.lower() or cond.lower() in text.lower():
                        return normalize_condition(cond)

        # Fallback: Regex im Text
        text = element.get_text()
        for cond in conditions:
            if cond.lower() in text.lower():
                return normalize_condition(cond)

        return None

    def _extract_language(self, element) -> Optional[str]:
        """Extrahiert die Kartensprache."""
        lang_map = {
            'german': 'German', 'deutsch': 'German', 'de': 'German',
            'english': 'English', 'englisch': 'English', 'en': 'English',
            'french': 'French', 'französisch': 'French', 'fr': 'French',
            'spanish': 'Spanish', 'spanisch': 'Spanish', 'es': 'Spanish',
            'italian': 'Italian', 'italienisch': 'Italian', 'it': 'Italian',
            'japanese': 'Japanese', 'japanisch': 'Japanese', 'jp': 'Japanese',
            'chinese': 'Chinese', 'chinesisch': 'Chinese',
            'korean': 'Korean', 'koreanisch': 'Korean',
            'portuguese': 'Portuguese', 'portugiesisch': 'Portuguese',
        }

        # Suche nach Flaggen-Icons oder Sprach-Tags
        lang_selectors = [
            'span[data-original-title*="Language"]',
            'span[class*="flag"]',
            'img[src*="flag"]',
            '[class*="language"]',
        ]

        for selector in lang_selectors:
            lang_el = element.select_one(selector)
            if lang_el:
                title = lang_el.get('data-original-title', '') or lang_el.get('title', '')
                src = lang_el.get('src', '')

                check_str = (title + src).lower()
                for key, val in lang_map.items():
                    if key in check_str:
                        return val

        return None

    def _extract_foil(self, element) -> bool:
        """Prüft ob es sich um eine Foil-Karte handelt."""
        foil_indicators = ['foil', 'holo', 'holographic']

        classes = ' '.join(element.get('class', []))
        text = element.get_text().lower()

        for indicator in foil_indicators:
            if indicator in classes.lower() or indicator in text:
                return True

        # Suche nach Foil-Icon
        foil_el = element.select_one('[class*="foil"], [data-original-title*="Foil"]')
        if foil_el:
            return True

        return False

    def _extract_seller_info(self, element) -> Dict[str, Any]:
        """Extrahiert Verkäufer-Informationen."""
        info = {}

        # Verkäufername
        seller_selectors = [
            'a[href*="/Users/"]',
            '.seller-name a',
            '[class*="seller"] a',
        ]

        for selector in seller_selectors:
            seller_el = element.select_one(selector)
            if seller_el:
                info['name'] = seller_el.get_text().strip()
                href = seller_el.get('href', '')

                # ID aus URL extrahieren
                id_match = re.search(r'/Users/([^/]+)', href)
                if id_match:
                    info['id'] = id_match.group(1)
                break

        # Rating
        rating_selectors = [
            'span[class*="seller-rating"]',
            '[class*="rating"]',
            'span[title*="%"]',
        ]

        for selector in rating_selectors:
            rating_el = element.select_one(selector)
            if rating_el:
                rating_text = rating_el.get('title', '') or rating_el.get_text()
                rating = parse_rating(rating_text)
                if rating:
                    info['rating'] = rating
                    break

        # Verkäufe
        sales_selectors = [
            '[class*="sell-count"]',
            'span[title*="sales"]',
            'span[title*="Verkäufe"]',
        ]

        for selector in sales_selectors:
            sales_el = element.select_one(selector)
            if sales_el:
                sales = parse_int(sales_el.get_text())
                if sales:
                    info['sales'] = sales
                    break

        # Land
        country_selectors = [
            'span[class*="flag-icon"]',
            'img[src*="flag"]',
            '[data-original-title*="Country"]',
        ]

        for selector in country_selectors:
            country_el = element.select_one(selector)
            if country_el:
                classes = ' '.join(country_el.get('class', []))
                title = country_el.get('data-original-title', '')
                src = country_el.get('src', '')

                # Country-Code aus Klasse oder URL extrahieren
                country_match = re.search(r'flag-icon-(\w{2})|/(\w{2})\.', classes + src)
                if country_match:
                    info['country'] = (country_match.group(1) or country_match.group(2)).upper()
                    break

        return info

    def _extract_article_info(self, element) -> Tuple[Optional[str], Optional[str]]:
        """Extrahiert Artikel-URL und ID."""
        article_url = None
        article_id = None

        # Suche nach direktem Artikel-Link
        link_selectors = [
            'a[href*="/Article/"]',
            'a[href*="/article/"]',
            'a.article-link',
        ]

        for selector in link_selectors:
            link_el = element.select_one(selector)
            if link_el:
                href = link_el.get('href', '')
                if href:
                    if not href.startswith('http'):
                        href = 'https://www.cardmarket.com' + href
                    article_url = href

                    # ID aus URL
                    id_match = re.search(r'/Article/(\d+)', href, re.I)
                    if id_match:
                        article_id = id_match.group(1)
                    break

        # Alternative: data-Attribute
        if not article_id:
            article_id = element.get('data-article-id') or element.get('data-id')

        return article_url, article_id

    def _extract_flags(self, element) -> Dict[str, Any]:
        """Extrahiert zusätzliche Flags."""
        flags = {}

        # Professional Seller
        if element.select_one('[class*="professional"], [title*="Professional"]'):
            flags['professional'] = True

        # Powerseller
        if element.select_one('[class*="powerseller"], [title*="Powerseller"]'):
            flags['powerseller'] = True

        # On Vacation
        if element.select_one('[class*="vacation"], [title*="Vacation"]'):
            flags['onVacation'] = True

        # First Edition, etc.
        if 'first edition' in element.get_text().lower():
            flags['firstEdition'] = True

        return flags if flags else {}

    def extract_product_id(self) -> Optional[str]:
        """Extrahiert die Produkt-ID aus der Seite."""
        # Meta-Tags
        meta = self.soup.find('meta', {'property': 'og:url'})
        if meta:
            content = meta.get('content', '')
            match = re.search(r'/Products/(\d+)', content)
            if match:
                return match.group(1)

        # Canonical URL
        canonical = self.soup.find('link', {'rel': 'canonical'})
        if canonical:
            href = canonical.get('href', '')
            match = re.search(r'/Products/(\d+)', href)
            if match:
                return match.group(1)

        return None


# ============================================
# AGGREGATION
# ============================================

def calculate_aggregates(offers: List[Offer]) -> AggregatedStats:
    """Berechnet aggregierte Statistiken aus den Angeboten."""
    stats = AggregatedStats()

    if not offers:
        return stats

    stats.offer_count = len(offers)

    # Eindeutige Verkäufer zählen
    sellers = set()
    for offer in offers:
        if offer.seller_id:
            sellers.add(offer.seller_id)
        elif offer.seller_name:
            sellers.add(offer.seller_name)
    stats.seller_count = len(sellers)

    # Preise sammeln (total bevorzugt, sonst price_item)
    prices = []
    for offer in offers:
        price = offer.total if offer.total else offer.price_item
        if price:
            prices.append(float(price))

    if not prices:
        return stats

    sorted_prices = sorted(prices)

    # Basis-Statistiken
    stats.min_total = Decimal(str(sorted_prices[0])).quantize(Decimal('0.01'))
    stats.max_total = Decimal(str(sorted_prices[-1])).quantize(Decimal('0.01'))
    stats.mean_total = Decimal(str(statistics.mean(prices))).quantize(Decimal('0.01'))

    # Perzentile
    stats.p10_total = calculate_percentile(sorted_prices, 10)
    stats.p25_total = calculate_percentile(sorted_prices, 25)
    stats.median_total = calculate_percentile(sorted_prices, 50)
    stats.p75_total = calculate_percentile(sorted_prices, 75)
    stats.p90_total = calculate_percentile(sorted_prices, 90)

    # IQR
    if stats.p25_total and stats.p75_total:
        stats.iqr_total = stats.p75_total - stats.p25_total

    # Standardabweichung
    if len(prices) > 1:
        stats.stdev_total = Decimal(str(statistics.stdev(prices))).quantize(Decimal('0.01'))

    # Getrimmter Mittelwert
    stats.trimmed_mean_total = calculate_trimmed_mean(prices)

    # Modus (häufigster Preis)
    try:
        stats.mode_total = Decimal(str(statistics.mode(prices))).quantize(Decimal('0.01'))
    except statistics.StatisticsError:
        pass

    return stats


# ============================================
# FLARESOLVERR
# ============================================

class FlareSolverrClient:
    """Client für FlareSolverr-Anfragen."""

    def __init__(self, base_url: str = FLARESOLVERR_URL, session_id: str = SESSION_ID):
        self.base_url = base_url
        self.session_id = session_id
        self.session_active = False

    def create_session(self) -> bool:
        """Erstellt eine neue Session."""
        try:
            # Alte Session löschen
            requests.post(self.base_url, json={
                'cmd': 'sessions.destroy',
                'session': self.session_id
            }, timeout=30)
        except Exception:
            pass

        try:
            response = requests.post(self.base_url, json={
                'cmd': 'sessions.create',
                'session': self.session_id
            }, timeout=60)

            data = response.json()
            self.session_active = data.get('status') == 'ok'
            return self.session_active

        except Exception as e:
            logger.error(f"Session-Erstellung fehlgeschlagen: {e}")
            return False

    def destroy_session(self):
        """Zerstört die aktuelle Session."""
        try:
            requests.post(self.base_url, json={
                'cmd': 'sessions.destroy',
                'session': self.session_id
            }, timeout=30)
        except Exception:
            pass
        self.session_active = False

    def fetch(self, url: str, timeout: int = REQUEST_TIMEOUT_SEC) -> Tuple[Optional[str], Optional[int], Optional[str]]:
        """
        Ruft eine URL ab.

        Returns:
            Tuple von (html, http_status, error_message)
        """
        try:
            response = requests.post(self.base_url, json={
                'cmd': 'request.get',
                'url': url,
                'session': self.session_id,
                'maxTimeout': timeout * 1000
            }, timeout=timeout + 30)

            data = response.json()

            if data.get('status') == 'ok' and data.get('solution'):
                solution = data['solution']
                html = solution.get('response', '')
                status = solution.get('status', 200)
                return html, status, None
            else:
                error = data.get('message', 'Unbekannter FlareSolverr-Fehler')
                return None, None, error

        except requests.exceptions.Timeout:
            return None, None, 'Timeout bei FlareSolverr-Anfrage'
        except requests.exceptions.ConnectionError:
            return None, None, 'FlareSolverr nicht erreichbar'
        except Exception as e:
            return None, None, str(e)

    def fetch_with_retry(self, url: str, max_retries: int = MAX_RETRIES) -> Tuple[Optional[str], Optional[int], Optional[str]]:
        """Fetch mit exponential backoff."""
        last_error = None

        for attempt in range(max_retries):
            html, status, error = self.fetch(url)

            if html is not None:
                return html, status, None

            last_error = error

            if attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)  # 2, 4, 8 Sekunden
                logger.warning(f"Retry {attempt + 1}/{max_retries} in {wait_time}s: {error}")
                time.sleep(wait_time)

        return None, None, last_error


# ============================================
# DATENBANK
# ============================================

class DatabaseManager:
    """Manager für Datenbankoperationen."""

    def __init__(self):
        self.pool = pooling.MySQLConnectionPool(
            pool_name="cardmarket_pool",
            pool_size=5,
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci',
            autocommit=False
        )

    def get_connection(self):
        """Holt eine Connection aus dem Pool."""
        return self.pool.get_connection()

    def get_watchlist(self) -> List[Dict]:
        """Lädt die aktive Watchlist."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT id, karten_nummer, set_code, land, foil
                FROM watchlist
                WHERE aktiv = TRUE
            """)
            return cursor.fetchall()
        finally:
            conn.close()

    def create_scan_run(
        self,
        watchlist_id: Optional[int],
        product_url: str,
        card_name: Optional[str],
        set_code: str,
        karten_nummer: str,
        land: str,
        foil: bool
    ) -> int:
        """Erstellt einen neuen scan_run Eintrag."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO scan_run
                (ts, watchlist_id, product_url, card_name, set_code, karten_nummer, land, foil, ok, parse_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, %s)
            """, (
                utc_now(),
                watchlist_id,
                product_url,
                card_name,
                set_code,
                karten_nummer,
                land,
                1 if foil else 0,
                PARSE_VERSION
            ))
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def update_scan_run(
        self,
        scan_id: int,
        ok: bool,
        http_status: Optional[int] = None,
        error: Optional[str] = None,
        product_id: Optional[str] = None
    ):
        """Aktualisiert einen scan_run Eintrag."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE scan_run
                SET ok = %s, http_status = %s, error = %s, product_id = %s
                WHERE id = %s
            """, (
                1 if ok else 0,
                http_status,
                error[:65000] if error else None,  # TEXT limit
                product_id,
                scan_id
            ))
            conn.commit()
        finally:
            conn.close()

    def bulk_insert_offers(self, scan_id: int, offers: List[Offer]):
        """Fügt mehrere Angebote per Bulk-Insert ein."""
        if not offers:
            return

        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            sql = """
                INSERT INTO offer_snapshot
                (scan_id, position, article_url, article_id, price_item, shipping, total,
                 currency, quantity, `condition`, language, is_foil, seller_name, seller_id,
                 seller_country, seller_rating, seller_sales, flags_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            values = []
            for offer in offers:
                values.append((
                    scan_id,
                    offer.position,
                    offer.article_url,
                    offer.article_id,
                    float(offer.price_item) if offer.price_item else None,
                    float(offer.shipping) if offer.shipping else None,
                    float(offer.total) if offer.total else None,
                    offer.currency,
                    offer.quantity,
                    offer.condition,
                    offer.language,
                    1 if offer.is_foil else 0,
                    offer.seller_name,
                    offer.seller_id,
                    offer.seller_country,
                    offer.seller_rating,
                    offer.seller_sales,
                    json.dumps(offer.flags) if offer.flags else None
                ))

            cursor.executemany(sql, values)
            conn.commit()

        finally:
            conn.close()

    def insert_scan_agg(self, scan_id: int, stats: AggregatedStats):
        """Fügt aggregierte Statistiken ein."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO scan_agg
                (scan_id, offer_count, seller_count, min_total, p10_total, p25_total,
                 median_total, p75_total, p90_total, max_total, trimmed_mean_total,
                 iqr_total, stdev_total, mean_total, mode_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                scan_id,
                stats.offer_count,
                stats.seller_count,
                float(stats.min_total) if stats.min_total else None,
                float(stats.p10_total) if stats.p10_total else None,
                float(stats.p25_total) if stats.p25_total else None,
                float(stats.median_total) if stats.median_total else None,
                float(stats.p75_total) if stats.p75_total else None,
                float(stats.p90_total) if stats.p90_total else None,
                float(stats.max_total) if stats.max_total else None,
                float(stats.trimmed_mean_total) if stats.trimmed_mean_total else None,
                float(stats.iqr_total) if stats.iqr_total else None,
                float(stats.stdev_total) if stats.stdev_total else None,
                float(stats.mean_total) if stats.mean_total else None,
                float(stats.mode_total) if stats.mode_total else None
            ))
            conn.commit()
        finally:
            conn.close()

    def get_rolling_baseline(
        self,
        karten_nummer: str,
        set_code: str,
        land: str,
        foil: bool,
        window_scans: int = BASELINE_WINDOW_SCANS
    ) -> Optional[Decimal]:
        """Berechnet den Rolling-Median der letzten N Scans."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT sa.median_total
                FROM scan_run sr
                INNER JOIN scan_agg sa ON sr.id = sa.scan_id
                WHERE sr.karten_nummer = %s
                AND sr.set_code = %s
                AND sr.land = %s
                AND sr.foil = %s
                AND sr.ok = 1
                AND sa.median_total IS NOT NULL
                ORDER BY sr.ts DESC
                LIMIT %s
            """, (karten_nummer, set_code, land, 1 if foil else 0, window_scans))

            rows = cursor.fetchall()

            if not rows:
                return None

            medians = [float(row[0]) for row in rows]
            baseline = statistics.median(medians)

            return Decimal(str(baseline)).quantize(Decimal('0.01'))

        finally:
            conn.close()

    def insert_deal_alert(
        self,
        scan_id: int,
        offer: Offer,
        baseline: Decimal,
        discount_pct: Decimal,
        reason: str,
        card_name: Optional[str],
        set_code: str,
        karten_nummer: str,
        land: str,
        foil: bool
    ):
        """Erstellt einen Deal-Alert."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO deal_alert
                (ts, scan_id, article_id, article_url, total, baseline, discount_pct,
                 reason, card_name, set_code, karten_nummer, land, foil, seller_name,
                 `condition`, meta_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                utc_now(),
                scan_id,
                offer.article_id,
                offer.article_url,
                float(offer.total) if offer.total else float(offer.price_item),
                float(baseline),
                float(discount_pct),
                reason,
                card_name,
                set_code,
                karten_nummer,
                land,
                1 if foil else 0,
                offer.seller_name,
                offer.condition,
                json.dumps({
                    'seller_rating': offer.seller_rating,
                    'seller_sales': offer.seller_sales,
                    'quantity': offer.quantity,
                    'language': offer.language
                })
            ))
            conn.commit()
        finally:
            conn.close()

    def save_legacy_price_history(
        self,
        card_name: str,
        card_number: str,
        set_code: str,
        min_price: float,
        avg_price: float,
        max_price: float,
        num_offers: int,
        country: str,
        is_foil: bool
    ):
        """Speichert in der alten preis_historie Tabelle (Kompatibilität)."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO preis_historie
                (karten_name, karten_nummer, set_code, min_preis, avg_preis, max_preis,
                 anzahl_angebote, land, foil, zeitstempel)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                card_name, card_number, set_code,
                min_price, avg_price, max_price,
                num_offers, country, 1 if is_foil else 0,
                utc_now()
            ))
            conn.commit()
        finally:
            conn.close()


# ============================================
# DEAL DETECTION
# ============================================

class DealDetector:
    """Erkennt Deals basierend auf Rolling-Baseline."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    def detect_deals(
        self,
        scan_id: int,
        offers: List[Offer],
        card_name: Optional[str],
        set_code: str,
        karten_nummer: str,
        land: str,
        foil: bool
    ) -> List[Offer]:
        """
        Erkennt Deals unter den Angeboten.

        Returns:
            Liste der als Deal erkannten Offers
        """
        baseline = self.db.get_rolling_baseline(
            karten_nummer, set_code, land, foil, BASELINE_WINDOW_SCANS
        )

        if not baseline:
            logger.debug(f"Keine Baseline verfügbar für {card_name} - überspringe Deal-Detection")
            return []

        threshold_price = baseline * Decimal(str(1 - DEAL_THRESHOLD))
        deals = []

        for offer in offers:
            # Preis ermitteln
            price = offer.total if offer.total else offer.price_item
            if not price:
                continue

            # Prüfe ob unter Threshold
            if price > threshold_price:
                continue

            # Prüfe Seller-Rating
            if offer.seller_rating is not None and offer.seller_rating < MIN_SELLER_RATING:
                continue

            # Prüfe Condition
            if offer.condition and not condition_meets_minimum(offer.condition, MIN_CONDITION):
                continue

            # Deal gefunden!
            discount_pct = (price - baseline) / baseline
            reason = f"Preis {float(price):.2f}€ ist {abs(float(discount_pct)*100):.1f}% unter Baseline {float(baseline):.2f}€"

            self.db.insert_deal_alert(
                scan_id=scan_id,
                offer=offer,
                baseline=baseline,
                discount_pct=discount_pct,
                reason=reason,
                card_name=card_name,
                set_code=set_code,
                karten_nummer=karten_nummer,
                land=land,
                foil=foil
            )

            deals.append(offer)
            logger.info(f"  DEAL: {card_name} - {float(price):.2f}€ ({float(discount_pct)*100:.1f}% Rabatt)")

        return deals


# ============================================
# HAUPT-SCANNER
# ============================================

class CardmarketScanner:
    """Hauptklasse für den Cardmarket-Scanner."""

    def __init__(self):
        self.db = DatabaseManager()
        self.client = FlareSolverrClient()
        self.deal_detector = DealDetector(self.db)

    def scan_card(
        self,
        watchlist_id: Optional[int],
        card_number: str,
        set_code: str,
        country: str,
        is_foil: bool
    ) -> bool:
        """Scannt eine einzelne Karte."""
        card_name = get_card_name(card_number, set_code)

        if not card_name:
            logger.warning(f"Karte nicht gefunden: {card_number} ({set_code})")
            return False

        url = generate_cardmarket_url(set_code, card_name, country, is_foil)
        logger.info(f"Scanne: {card_name} ({set_code}) [{country}]{' FOIL' if is_foil else ''}")

        # scan_run erstellen
        scan_id = self.db.create_scan_run(
            watchlist_id=watchlist_id,
            product_url=url,
            card_name=card_name,
            set_code=set_code,
            karten_nummer=card_number,
            land=country,
            foil=is_foil
        )

        try:
            # HTML abrufen
            html, http_status, error = self.client.fetch_with_retry(url)

            if not html:
                self.db.update_scan_run(scan_id, ok=False, http_status=http_status, error=error)
                logger.warning(f"  Fehler: {error}")
                return False

            # Prüfen ob Seite gefunden wurde
            if 'Product not found' in html or 'Page not found' in html:
                self.db.update_scan_run(
                    scan_id, ok=False, http_status=404,
                    error='Produkt nicht auf Cardmarket gefunden'
                )
                logger.warning(f"  Produkt nicht gefunden")
                return False

            # HTML parsen
            parser = CardmarketParser(html)
            offers = parser.parse(MAX_OFFERS_PER_SCAN)
            product_id = parser.extract_product_id()

            if not offers:
                self.db.update_scan_run(
                    scan_id, ok=False, http_status=http_status,
                    error='Keine Angebote gefunden', product_id=product_id
                )
                logger.warning(f"  Keine Angebote gefunden")
                return False

            # Angebote speichern
            self.db.bulk_insert_offers(scan_id, offers)

            # Aggregation berechnen und speichern
            stats = calculate_aggregates(offers)
            self.db.insert_scan_agg(scan_id, stats)

            # scan_run als erfolgreich markieren
            self.db.update_scan_run(
                scan_id, ok=True, http_status=http_status, product_id=product_id
            )

            # Legacy-Kompatibilität: auch in preis_historie schreiben
            if stats.min_total and stats.mean_total and stats.max_total:
                self.db.save_legacy_price_history(
                    card_name=card_name,
                    card_number=card_number,
                    set_code=set_code,
                    min_price=float(stats.min_total),
                    avg_price=float(stats.mean_total),
                    max_price=float(stats.max_total),
                    num_offers=stats.offer_count,
                    country=country,
                    is_foil=is_foil
                )

            # Deal-Detection
            deals = self.deal_detector.detect_deals(
                scan_id=scan_id,
                offers=offers,
                card_name=card_name,
                set_code=set_code,
                karten_nummer=card_number,
                land=country,
                foil=is_foil
            )

            # Logging
            logger.info(
                f"  OK: {stats.offer_count} Angebote, "
                f"Median: {stats.median_total}€, "
                f"Range: {stats.min_total}€ - {stats.max_total}€"
                + (f", {len(deals)} Deals!" if deals else "")
            )

            return True

        except Exception as e:
            self.db.update_scan_run(scan_id, ok=False, error=str(e))
            logger.error(f"  Exception: {e}")
            return False

    def run_scheduled_scan(self):
        """Führt einen vollständigen Watchlist-Scan durch."""
        logger.info("=" * 60)
        logger.info("Cardmarket Scanner v2 - Geplanter Scan")
        logger.info(f"Zeitpunkt: {utc_now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        logger.info(f"Parser-Version: {PARSE_VERSION}")
        logger.info("=" * 60)

        # Watchlist laden
        watchlist = self.db.get_watchlist()

        if not watchlist:
            logger.info("Watchlist ist leer.")
            return

        logger.info(f"Watchlist: {len(watchlist)} Karten")

        # FlareSolverr-Session erstellen
        if not self.client.create_session():
            logger.error("Konnte FlareSolverr-Session nicht erstellen. Abbruch.")
            return

        try:
            scanned = 0
            errors = 0

            for item in watchlist:
                try:
                    success = self.scan_card(
                        watchlist_id=item['id'],
                        card_number=item['karten_nummer'],
                        set_code=item['set_code'],
                        country=item['land'],
                        is_foil=bool(item['foil'])
                    )

                    if success:
                        scanned += 1
                    else:
                        errors += 1

                    # Rate limiting
                    time.sleep(SLEEP_BETWEEN_CARDS_SEC)

                except Exception as e:
                    logger.error(f"Fehler bei Karte {item['karten_nummer']}: {e}")
                    errors += 1

            logger.info("-" * 60)
            logger.info(f"Scan abgeschlossen: {scanned} erfolgreich, {errors} Fehler")

        finally:
            self.client.destroy_session()
            logger.info("Session beendet")

    def test_single_card(self, card_number: str, set_code: str, country: str, is_foil: bool = False):
        """Testet eine einzelne Karte ohne DB-Speicherung (außer scan_run)."""
        card_name = get_card_name(card_number, set_code)

        if not card_name:
            print(f"Karte nicht gefunden: {card_number} ({set_code})")
            return

        url = generate_cardmarket_url(set_code, card_name, country, is_foil)
        print(f"\nTeste: {card_name} ({set_code})")
        print(f"URL: {url}")

        if not self.client.create_session():
            print("FlareSolverr nicht erreichbar!")
            return

        try:
            html, http_status, error = self.client.fetch_with_retry(url)

            if not html:
                print(f"Fehler: {error}")
                return

            print(f"HTTP Status: {http_status}")

            parser = CardmarketParser(html)
            offers = parser.parse(MAX_OFFERS_PER_SCAN)

            if not offers:
                print("Keine Angebote gefunden")
                return

            stats = calculate_aggregates(offers)

            print(f"\n{stats.offer_count} Angebote gefunden ({stats.seller_count} Verkäufer)")
            print(f"\nPreisstatistiken:")
            print(f"  Min:     {stats.min_total}€")
            print(f"  P10:     {stats.p10_total}€")
            print(f"  P25:     {stats.p25_total}€")
            print(f"  Median:  {stats.median_total}€")
            print(f"  P75:     {stats.p75_total}€")
            print(f"  P90:     {stats.p90_total}€")
            print(f"  Max:     {stats.max_total}€")
            print(f"  IQR:     {stats.iqr_total}€")
            print(f"  StdDev:  {stats.stdev_total}€")
            print(f"  Trimmed Mean: {stats.trimmed_mean_total}€")

            print(f"\nErste 5 Angebote:")
            for offer in offers[:5]:
                foil_str = " [FOIL]" if offer.is_foil else ""
                cond_str = f" [{offer.condition}]" if offer.condition else ""
                seller_str = f" von {offer.seller_name}" if offer.seller_name else ""
                print(f"  {offer.position}. {offer.total or offer.price_item}€{foil_str}{cond_str}{seller_str}")

        finally:
            self.client.destroy_session()


# ============================================
# CLI INTERFACE
# ============================================

def main():
    """Hauptfunktion."""
    scanner = CardmarketScanner()

    if len(sys.argv) < 2:
        scanner.run_scheduled_scan()
        return

    command = sys.argv[1]

    if command == 'scan':
        scanner.run_scheduled_scan()

    elif command == 'test':
        if len(sys.argv) < 4:
            print("Verwendung: python cron_scanner_v2.py test <karten_nummer> <set_code> [land] [foil]")
            print("Beispiel:   python cron_scanner_v2.py test 257 OGN DE")
            print("            python cron_scanner_v2.py test 257 OGN DE foil")
            return

        card_number = sys.argv[2]
        set_code = sys.argv[3]
        country = sys.argv[4] if len(sys.argv) > 4 else 'DE'
        is_foil = len(sys.argv) > 5 and sys.argv[5].lower() in ['true', '1', 'yes', 'foil']

        scanner.test_single_card(card_number, set_code, country, is_foil)

    elif command == 'add':
        if len(sys.argv) < 4:
            print("Verwendung: python cron_scanner_v2.py add <karten_nummer> <set_code> [land] [foil]")
            return

        card_number = sys.argv[2]
        set_code = sys.argv[3]
        country = sys.argv[4] if len(sys.argv) > 4 else 'DE'
        is_foil = len(sys.argv) > 5 and sys.argv[5].lower() in ['true', '1', 'yes', 'foil']

        conn = scanner.db.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT IGNORE INTO watchlist (karten_nummer, set_code, land, foil)
                VALUES (%s, %s, %s, %s)
            """, (card_number, set_code, country, 1 if is_foil else 0))
            conn.commit()

            card_name = get_card_name(card_number, set_code)
            print(f"Hinzugefügt: {card_name or card_number} ({set_code}) - {country}{' FOIL' if is_foil else ''}")
        finally:
            conn.close()

    elif command == 'list':
        watchlist = scanner.db.get_watchlist()

        if not watchlist:
            print("Watchlist ist leer.")
            return

        print(f"\nWatchlist ({len(watchlist)} Karten):\n")
        for item in watchlist:
            card_name = get_card_name(item['karten_nummer'], item['set_code'])
            foil_str = " [FOIL]" if item['foil'] else ""
            print(f"  {card_name or item['karten_nummer']} ({item['set_code']}) - {item['land']}{foil_str}")

    elif command == 'deals':
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 1

        conn = scanner.db.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT *
                FROM deal_alert
                WHERE ts >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY ts DESC
                LIMIT 50
            """, (days,))

            deals = cursor.fetchall()

            if not deals:
                print(f"Keine Deals in den letzten {days} Tagen.")
                return

            print(f"\nDeals der letzten {days} Tage ({len(deals)} gefunden):\n")
            for deal in deals:
                print(f"  {deal['ts']} | {deal['card_name']} ({deal['set_code']})")
                print(f"    Preis: {deal['total']}€ (Baseline: {deal['baseline']}€, {deal['discount_pct']*100:.1f}%)")
                print(f"    {deal['reason']}")
                if deal['article_url']:
                    print(f"    Link: {deal['article_url']}")
                print()
        finally:
            conn.close()

    elif command == 'stats':
        if len(sys.argv) < 4:
            print("Verwendung: python cron_scanner_v2.py stats <karten_nummer> <set_code> [land] [days]")
            return

        card_number = sys.argv[2]
        set_code = sys.argv[3]
        country = sys.argv[4] if len(sys.argv) > 4 else 'DE'
        days = int(sys.argv[5]) if len(sys.argv) > 5 else 7

        card_name = get_card_name(card_number, set_code)

        conn = scanner.db.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT sr.ts, sa.*
                FROM scan_run sr
                INNER JOIN scan_agg sa ON sr.id = sa.scan_id
                WHERE sr.karten_nummer = %s
                AND sr.set_code = %s
                AND sr.land = %s
                AND sr.ok = 1
                AND sr.ts >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY sr.ts DESC
                LIMIT 100
            """, (card_number, set_code, country, days))

            rows = cursor.fetchall()

            if not rows:
                print(f"Keine Daten für {card_name} ({set_code}) in den letzten {days} Tagen.")
                return

            print(f"\nStatistiken für {card_name} ({set_code}) [{country}]")
            print(f"Letzte {days} Tage, {len(rows)} Scans\n")

            medians = [float(r['median_total']) for r in rows if r['median_total']]
            mins = [float(r['min_total']) for r in rows if r['min_total']]

            if medians:
                print(f"Median-Bereich:  {min(medians):.2f}€ - {max(medians):.2f}€")
                print(f"Durchschn. Median: {statistics.mean(medians):.2f}€")

            if mins:
                print(f"Min-Bereich:     {min(mins):.2f}€ - {max(mins):.2f}€")
                print(f"Durchschn. Min:  {statistics.mean(mins):.2f}€")

            baseline = scanner.db.get_rolling_baseline(card_number, set_code, country, False)
            if baseline:
                print(f"\nAktuelle Baseline (Rolling-Median): {baseline}€")
                print(f"Deal-Schwelle ({DEAL_THRESHOLD*100:.0f}%): {float(baseline) * (1-DEAL_THRESHOLD):.2f}€")

        finally:
            conn.close()

    else:
        print("Cardmarket Scanner v2")
        print("=" * 40)
        print(f"Parser-Version: {PARSE_VERSION}")
        print("\nBefehle:")
        print("  scan       - Watchlist-Scan durchführen")
        print("  test       - Einzelne Karte testen")
        print("  add        - Karte zur Watchlist hinzufügen")
        print("  list       - Watchlist anzeigen")
        print("  deals      - Aktuelle Deals anzeigen")
        print("  stats      - Statistiken für eine Karte")
        print("\nBeispiele:")
        print("  python cron_scanner_v2.py scan")
        print("  python cron_scanner_v2.py test 257 OGN DE")
        print("  python cron_scanner_v2.py add 12 OGN DE foil")
        print("  python cron_scanner_v2.py deals 7")
        print("  python cron_scanner_v2.py stats 257 OGN DE 14")


if __name__ == '__main__':
    main()
