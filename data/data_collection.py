#!/usr/bin/env python3
"""
Digital Alexandria - Heritage Data Collection Module

This module collects cultural heritage data from various sources:
- Museum APIs (Google Arts & Culture, Metropolitan Museum)
- UNESCO World Heritage Sites
- News monitoring for cultural threats
- Social media sentiment analysis

Author: William Couturier
Date: 2024
"""

import requests
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import os
from dataclasses import dataclass
import sqlite3
import hashlib

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('heritage_data_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class HeritageItem:
    """Data class for cultural heritage items"""
    name: str
    location: str
    type: str  # artwork, monument, document, etc.
    period: str
    significance_score: float
    threat_level: str
    last_updated: datetime
    source: str
    metadata: Dict


class HeritageDataCollector:
    """Main class for collecting cultural heritage data"""

    def __init__(self, config_path: str = "config.json"):
        """Initialize the data collector with configuration"""
        self.config = self._load_config(config_path)
        self.db_path = self.config.get('database_path', 'heritage_data.db')
        self._init_database()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Digital-Alexandria-Heritage-Collector/1.0'
        })
        logger.info("🏛️ Digital Alexandria Data Collector initialized")

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from file"""
        default_config = {
            'met_museum_api': 'https://collectionapi.metmuseum.org/public/collection/v1',
            'unesco_api': 'https://whc.unesco.org/en/list/json/',
            'news_api_key': os.getenv('NEWS_API_KEY'),
            'rate_limit_delay': 1.0,
            'batch_size': 100,
            'database_path': 'heritage_data.db'
        }

        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                default_config.update(user_config)

        return default_config

    def _init_database(self):
        """Initialize SQLite database for storing heritage data"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Heritage items table
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS heritage_items
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               name
                               TEXT
                               NOT
                               NULL,
                               location
                               TEXT,
                               type
                               TEXT,
                               period
                               TEXT,
                               significance_score
                               REAL,
                               threat_level
                               TEXT,
                               last_updated
                               TIMESTAMP,
                               source
                               TEXT,
                               metadata
                               TEXT,
                               hash_id
                               TEXT
                               UNIQUE
                           )
                           ''')

            # Threat monitoring table
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS threat_monitoring
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               heritage_item_id
                               INTEGER,
                               threat_type
                               TEXT,
                               severity
                               TEXT,
                               description
                               TEXT,
                               detected_date
                               TIMESTAMP,
                               source
                               TEXT,
                               FOREIGN
                               KEY
                           (
                               heritage_item_id
                           ) REFERENCES heritage_items
                           (
                               id
                           )
                               )
                           ''')

            # Collection statistics table
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS collection_stats
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               collection_date
                               TIMESTAMP,
                               source
                               TEXT,
                               total_items
                               INTEGER,
                               new_items
                               INTEGER,
                               updated_items
                               INTEGER,
                               errors
                               INTEGER
                           )
                           ''')

            conn.commit()
            logger.info("✅ Database initialized successfully")

    def collect_met_museum_data(self, department_ids: List[int] = None) -> List[HeritageItem]:
        """
        Collect data from Metropolitan Museum API

        Args:
            department_ids: List of department IDs to focus on

        Returns:
            List of HeritageItem objects
        """
        logger.info("🎨 Starting Metropolitan Museum data collection...")
        heritage_items = []

        try:
            # Get all object IDs
            objects_url = f"{self.config['met_museum_api']}/objects"

            if department_ids:
                params = {'departmentIds': '|'.join(map(str, department_ids))}
                response = self.session.get(objects_url, params=params)
            else:
                response = self.session.get(objects_url)

            response.raise_for_status()
            object_ids = response.json().get('objectIDs', [])

            logger.info(f"📊 Found {len(object_ids)} objects to process")

            # Process objects in batches
            batch_size = self.config['batch_size']
            for i in range(0, min(len(object_ids), 1000), batch_size):  # Limit for demo
                batch_ids = object_ids[i:i + batch_size]
                batch_items = self._process_met_batch(batch_ids)
                heritage_items.extend(batch_items)

                # Rate limiting
                time.sleep(self.config['rate_limit_delay'])

                if i % (batch_size * 10) == 0:
                    logger.info(f"⏳ Processed {i}/{min(len(object_ids), 1000)} objects")

            logger.info(f"✅ Met Museum collection completed: {len(heritage_items)} items")

        except Exception as e:
            logger.error(f"❌ Error collecting Met Museum data: {str(e)}")

        return heritage_items

    def _process_met_batch(self, object_ids: List[int]) -> List[HeritageItem]:
        """Process a batch of Metropolitan Museum objects"""
        items = []

        for obj_id in object_ids:
            try:
                # Get object details
                obj_url = f"{self.config['met_museum_api']}/objects/{obj_id}"
                response = self.session.get(obj_url)
                response.raise_for_status()
                data = response.json()

                # Skip objects without essential information
                if not data.get('title') or not data.get('isPublicDomain'):
                    continue

                # Calculate significance score based on various factors
                significance = self._calculate_significance_score(data)

                # Determine threat level based on location and type
                threat_level = self._assess_threat_level(data)

                # Create heritage item
                item = HeritageItem(
                    name=data.get('title', 'Unknown'),
                    location=f"{data.get('city', '')}, {data.get('country', '')}".strip(', '),
                    type=data.get('classification', 'artwork'),
                    period=data.get('period', data.get('dynasty', 'Unknown')),
                    significance_score=significance,
                    threat_level=threat_level,
                    last_updated=datetime.now(),
                    source='Metropolitan Museum',
                    metadata={
                        'object_id': obj_id,
                        'artist': data.get('artistDisplayName'),
                        'date': data.get('objectDate'),
                        'medium': data.get('medium'),
                        'dimensions': data.get('dimensions'),
                        'department': data.get('department'),
                        'accession_number': data.get('accessionNumber'),
                        'public_domain': data.get('isPublicDomain'),
                        'primary_image': data.get('primaryImage'),
                        'gallery_number': data.get('GalleryNumber')
                    }
                )

                items.append(item)

            except Exception as e:
                logger.warning(f"⚠️ Error processing object {obj_id}: {str(e)}")
                continue

        return items

    def collect_unesco_data(self) -> List[HeritageItem]:
        """Collect UNESCO World Heritage Sites data"""
        logger.info("🏛️ Starting UNESCO World Heritage Sites collection...")
        heritage_items = []

        try:
            response = self.session.get(self.config['unesco_api'])
            response.raise_for_status()
            data = response.json()

            for site in data:
                # Calculate significance score
                significance = min(9.0 + (2024 - int(site.get('date_inscribed', 2024))) / 100, 10.0)

                # Assess threat level based on danger list status
                threat_level = 'High' if site.get('danger', '0') == '1' else 'Medium'

                item = HeritageItem(
                    name=site.get('site', 'Unknown Site'),
                    location=f"{site.get('states', '')}",
                    type='heritage_site',
                    period=site.get('date_inscribed', 'Unknown'),
                    significance_score=significance,
                    threat_level=threat_level,
                    last_updated=datetime.now(),
                    source='UNESCO',
                    metadata={
                        'unique_number': site.get('unique_number'),
                        'criteria': site.get('criteria'),
                        'category': site.get('category'),
                        'short_description': site.get('short_description'),
                        'longitude': site.get('longitude'),
                        'latitude': site.get('latitude'),
                        'area_hectares': site.get('area_hectares'),
                        'danger_list': site.get('danger', '0') == '1',
                        'transboundary': site.get('transboundary', '0') == '1'
                    }
                )

                heritage_items.append(item)

            logger.info(f"✅ UNESCO collection completed: {len(heritage_items)} sites")

        except Exception as e:
            logger.error(f"❌ Error collecting UNESCO data: {str(e)}")

        return heritage_items

    def monitor_cultural_threats(self, keywords: List[str] = None) -> List[Dict]:
        """
        Monitor news for cultural heritage threats

        Args:
            keywords: List of keywords to search for threats

        Returns:
            List of threat incidents
        """
        if not self.config.get('news_api_key'):
            logger.warning("⚠️ No News API key configured, skipping threat monitoring")
            return []

        if keywords is None:
            keywords = [
                'museum destroyed', 'cultural heritage damage', 'art theft',
                'monument vandalized', 'archaeological site damaged',
                'library burned', 'statue toppled', 'heritage site threatened'
            ]

        logger.info("📰 Starting cultural threat monitoring...")
        threats = []

        try:
            for keyword in keywords:
                # Search news from last 30 days
                params = {
                    'q': keyword,
                    'language': 'en',
                    'sortBy': 'relevancy',
                    'from': (datetime.now() - timedelta(days=30)).isoformat(),
                    'apiKey': self.config['news_api_key']
                }

                response = self.session.get(
                    'https://newsapi.org/v2/everything',
                    params=params
                )
                response.raise_for_status()
                data = response.json()

                for article in data.get('articles', []):
                    threat = {
                        'title': article.get('title'),
                        'description': article.get('description'),
                        'source': article.get('source', {}).get('name'),
                        'published_at': article.get('publishedAt'),
                        'url': article.get('url'),
                        'keyword': keyword,
                        'severity': self._assess_threat_severity(article)
                    }
                    threats.append(threat)

                time.sleep(self.config['rate_limit_delay'])

            logger.info(f"✅ Threat monitoring completed: {len(threats)} incidents found")

        except Exception as e:
            logger.error(f"❌ Error monitoring threats: {str(e)}")

        return threats

    def _calculate_significance_score(self, data: Dict) -> float:
        """Calculate cultural significance score for an artwork"""
        score = 5.0  # Base score

        # Artist recognition (simplified)
        famous_artists = [
            'Leonardo da Vinci', 'Michelangelo', 'Vincent van Gogh',
            'Pablo Picasso', 'Claude Monet', 'Rembrandt', 'Auguste Rodin'
        ]

        artist = data.get('artistDisplayName', '')
        if any(famous in artist for famous in famous_artists):
            score += 2.0

        # Age factor
        object_date = data.get('objectDate', '')
        if any(period in object_date.lower() for period in ['bc', 'century', 'dynasty']):
            score += 1.5

        # Public domain and accessibility
        if data.get('isPublicDomain'):
            score += 0.5

        # Has image
        if data.get('primaryImage'):
            score += 0.5

        # Currently on display
        if data.get('isOnView'):
            score += 0.5

        return min(score, 10.0)

    def _assess_threat_level(self, data: Dict) -> str:
        """Assess threat level for cultural items"""
        high_risk_locations = [
            'Syria', 'Iraq', 'Afghanistan', 'Yemen', 'Libya',
            'Ukraine', 'Myanmar', 'Mali'
        ]

        location = data.get('country', '').lower()

        if any(risk_loc.lower() in location for risk_loc in high_risk_locations):
            return 'High'
        elif data.get('isOnView', False):
            return 'Low'  # Museum security
        else:
            return 'Medium'

    def _assess_threat_severity(self, article: Dict) -> str:
        """Assess severity of threat based on article content"""
        high_severity_keywords = [
            'destroyed', 'burned', 'demolished', 'stolen', 'looted'
        ]
        medium_severity_keywords = [
            'damaged', 'vandalized', 'threatened', 'at risk'
        ]

        content = f"{article.get('title', '')} {article.get('description', '')}".lower()

        if any(keyword in content for keyword in high_severity_keywords):
            return 'High'
        elif any(keyword in content for keyword in medium_severity_keywords):
            return 'Medium'
        else:
            return 'Low'

    def save_to_database(self, heritage_items: List[HeritageItem]) -> Tuple[int, int]:
        """
        Save heritage items to database

        Returns:
            Tuple of (new_items, updated_items)
        """
        new_items = 0
        updated_items = 0

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            for item in heritage_items:
                # Create hash for deduplication
                item_data = f"{item.name}_{item.location}_{item.source}"
                hash_id = hashlib.md5(item_data.encode()).hexdigest()

                # Check if item exists
                cursor.execute(
                    "SELECT id FROM heritage_items WHERE hash_id = ?",
                    (hash_id,)
                )
                existing = cursor.fetchone()

                metadata_json = json.dumps(item.metadata)

                if existing:
                    # Update existing item
                    cursor.execute('''
                                   UPDATE heritage_items
                                   SET significance_score = ?,
                                       threat_level       = ?,
                                       last_updated       = ?,
                                       metadata           = ?
                                   WHERE hash_id = ?
                                   ''', (
                                       item.significance_score, item.threat_level,
                                       item.last_updated, metadata_json, hash_id
                                   ))
                    updated_items += 1
                else:
                    # Insert new item
                    cursor.execute('''
                                   INSERT INTO heritage_items
                                   (name, location, type, period, significance_score,
                                    threat_level, last_updated, source, metadata, hash_id)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                   ''', (
                                       item.name, item.location, item.type, item.period,
                                       item.significance_score, item.threat_level,
                                       item.last_updated, item.source, metadata_json, hash_id
                                   ))
                    new_items += 1

            conn.commit()

        logger.info(f"💾 Database updated: {new_items} new, {updated_items} updated")
        return new_items, updated_items

    def save_threats_to_database(self, threats: List[Dict]) -> int:
        """Save threat incidents to database"""
        saved_threats = 0

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            for threat in threats:
                cursor.execute('''
                               INSERT INTO threat_monitoring
                                   (threat_type, severity, description, detected_date, source)
                               VALUES (?, ?, ?, ?, ?)
                               ''', (
                                   threat.get('keyword', 'unknown'),
                                   threat.get('severity', 'unknown'),
                                   threat.get('title', ''),
                                   datetime.now(),
                                   threat.get('source', 'news')
                               ))
                saved_threats += 1

            conn.commit()

        logger.info(f"⚠️ Threats saved: {saved_threats} incidents")
        return saved_threats

    def generate_collection_report(self) -> Dict:
        """Generate a comprehensive collection report"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Total items by source
            cursor.execute('''
                           SELECT source,
                                  COUNT(*) as count, 
                       AVG(significance_score) as avg_significance
                           FROM heritage_items
                           GROUP BY source
                           ''')
            sources = cursor.fetchall()

            # Threat level distribution
            cursor.execute('''
                           SELECT threat_level, COUNT(*) as count
                           FROM heritage_items
                           GROUP BY threat_level
                           ''')
            threats = cursor.fetchall()

            # Recent threats
            cursor.execute('''
                           SELECT COUNT(*) as recent_threats
                           FROM threat_monitoring
                           WHERE detected_date > datetime('now', '-7 days')
                           ''')
            recent_threats = cursor.fetchone()[0]

            # Top locations
            cursor.execute('''
                           SELECT location, COUNT(*) as count
                           FROM heritage_items
                           WHERE location IS NOT NULL AND location != ''
                           GROUP BY location
                           ORDER BY count DESC
                               LIMIT 10
                           ''')
            top_locations = cursor.fetchall()

            report = {
                'collection_date': datetime.now().isoformat(),
                'total_items': sum(count for _, count, _ in sources),
                'sources': {source: {'count': count, 'avg_significance': avg_sig}
                            for source, count, avg_sig in sources},
                'threat_distribution': {level: count for level, count in threats},
                'recent_threats': recent_threats,
                'top_locations': {loc: count for loc, count in top_locations}
            }

        return report

    def run_full_collection(self) -> Dict:
        """Run a complete data collection cycle"""
        logger.info("🚀 Starting full heritage data collection cycle...")
        start_time = datetime.now()

        results = {
            'start_time': start_time.isoformat(),
            'sources_processed': [],
            'total_items_collected': 0,
            'new_items': 0,
            'updated_items': 0,
            'threats_detected': 0,
            'errors': []
        }

        try:
            # Collect Met Museum data (limited sample)
            met_items = self.collect_met_museum_data(department_ids=[1, 11, 21])  # Paintings, etc.
            new, updated = self.save_to_database(met_items)

            results['sources_processed'].append('Metropolitan Museum')
            results['total_items_collected'] += len(met_items)
            results['new_items'] += new
            results['updated_items'] += updated

        except Exception as e:
            error_msg = f"Met Museum collection failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            results['errors'].append(error_msg)

        try:
            # Collect UNESCO data
            unesco_items = self.collect_unesco_data()
            new, updated = self.save_to_database(unesco_items)

            results['sources_processed'].append('UNESCO')
            results['total_items_collected'] += len(unesco_items)
            results['new_items'] += new
            results['updated_items'] += updated

        except Exception as e:
            error_msg = f"UNESCO collection failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            results['errors'].append(error_msg)

        try:
            # Monitor threats
            threats = self.monitor_cultural_threats()
            threat_count = self.save_threats_to_database(threats)

            results['sources_processed'].append('Threat Monitoring')
            results['threats_detected'] = threat_count

        except Exception as e:
            error_msg = f"Threat monitoring failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            results['errors'].append(error_msg)

        # Record collection statistics
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                           INSERT INTO collection_stats
                           (collection_date, source, total_items, new_items, updated_items, errors)
                           VALUES (?, ?, ?, ?, ?, ?)
                           ''', (
                               start_time, 'full_collection',
                               results['total_items_collected'],
                               results['new_items'],
                               results['updated_items'],
                               len(results['errors'])
                           ))
            conn.commit()

        end_time = datetime.now()
        results['end_time'] = end_time.isoformat()
        results['duration_minutes'] = (end_time - start_time).total_seconds() / 60

        logger.info("✅ Full collection cycle completed!")
        logger.info(f"📊 Summary: {results['total_items_collected']} items, "
                    f"{results['new_items']} new, {results['updated_items']} updated, "
                    f"{results['threats_detected']} threats detected")

        return results


def main():
    """Main execution function"""
    print("🏛️ Digital Alexandria - Heritage Data Collector")
    print("=" * 50)

    # Initialize collector
    collector = HeritageDataCollector()

    # Run full collection
    results = collector.run_full_collection()

    # Generate and display report
    report = collector.generate_collection_report()

    print("\n📊 COLLECTION SUMMARY:")
    print(f"⏱️  Duration: {results['duration_minutes']:.1f} minutes")
    print(f"📦 Total items collected: {results['total_items_collected']}")
    print(f"🆕 New items: {results['new_items']}")
    print(f"🔄 Updated items: {results['updated_items']}")
    print(f"⚠️  Threats detected: {results['threats_detected']}")
    print(f"❌ Errors: {len(results['errors'])}")

    print("\n🗃️ DATABASE OVERVIEW:")
    print(f"📚 Total heritage items: {report['total_items']}")
    print("📊 By source:")
    for source, data in report['sources'].items():
        print(f"  • {source}: {data['count']} items (avg significance: {data['avg_significance']:.1f})")

    print("⚠️ Threat levels:")
    for level, count in report['threat_distribution'].items():
        print(f"  • {level}: {count} items")

    print(f"🚨 Recent threats (7 days): {report['recent_threats']}")

    if results['errors']:
        print("\n❌ ERRORS ENCOUNTERED:")
        for error in results['errors']:
            print(f"  • {error}")

    print("\n🌟 Digital Alexandria data collection completed!")
    print("Ready for analysis and AI model training.")


if __name__ == "__main__":
    main()