# app.py
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import pytz
import re

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, and_, func, text
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import atexit

# Import your existing Playwright scraper logic
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://localhost/carscraper')
# Fix for Railway PostgreSQL URL (postgres:// -> postgresql://)
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_pre_ping': True,
    'pool_recycle': 300,
}

db = SQLAlchemy(app)

# Timezone for CET
CET = pytz.timezone('Europe/Vienna')

# ============================================================================
# DATABASE MODELS
# ============================================================================

class Car(db.Model):
    __tablename__ = 'cars'
    
    id = db.Column(db.Integer, primary_key=True)
    listing_id = db.Column(db.String(100), unique=True, nullable=False, index=True)
    title = db.Column(db.String(500), nullable=False)
    price = db.Column(db.Numeric(10, 2))
    currency = db.Column(db.String(10), default='EUR')
    brand = db.Column(db.String(100), index=True)
    model = db.Column(db.String(100))
    year = db.Column(db.Integer)
    mileage = db.Column(db.Integer)
    fuel_type = db.Column(db.String(50))
    transmission = db.Column(db.String(50))
    location = db.Column(db.String(200))
    image_urls = db.Column(db.JSON)  # Store array of image URLs
    url = db.Column(db.Text, nullable=False)
    description = db.Column(db.Text)
    posted_at = db.Column(db.DateTime)  # When the car was posted on Willhaben
    posted_at = db.Column(db.DateTime)  # When the car was originally posted on Willhaben
    first_seen_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary for API responses"""
        return {
            'id': self.id,
            'listing_id': self.listing_id,
            'title': self.title,
            'price': float(self.price) if self.price else None,
            'currency': self.currency,
            'brand': self.brand,
            'model': self.model,
            'year': self.year,
            'mileage': self.mileage,
            'fuel_type': self.fuel_type,
            'transmission': self.transmission,
            'location': self.location,
            'image_urls': self.image_urls,  # Returns array like ["url1", "url2", ...]
            'url': self.url,
            'description': self.description,
            'posted_at': self.posted_at.isoformat() if self.posted_at else None,
            'first_seen_at': self.first_seen_at.isoformat() if self.first_seen_at else None,
            'last_seen_at': self.last_seen_at.isoformat() if self.last_seen_at else None,
            'is_active': self.is_active,
        }


class ScrapingLog(db.Model):
    __tablename__ = 'scraping_log'
    
    id = db.Column(db.Integer, primary_key=True)
    scrape_started_at = db.Column(db.DateTime, default=datetime.utcnow)
    scrape_completed_at = db.Column(db.DateTime)
    cars_found = db.Column(db.Integer, default=0)
    cars_added = db.Column(db.Integer, default=0)
    cars_updated = db.Column(db.Integer, default=0)
    status = db.Column(db.String(50))
    error_message = db.Column(db.Text)


# ============================================================================
# SCRAPER CLASS
# ============================================================================

class WillhabenScraper:
    """Scraper for willhaben.at car listings - Simplified robust version"""
    
    BASE_URL = "https://www.willhaben.at/iad/gebrauchtwagen/auto/gebrauchtwagenboerse?rows=30"
    
    def __init__(self, max_cars: int = 100, full_image_scraping: bool = False):
        self.max_cars = max_cars
        self.full_image_scraping = full_image_scraping  # Disabled by default for speed
    
    def scrape_listings(self) -> List[Dict[str, Any]]:
        """
        Scrape car listings from willhaben.at
        Returns list of car dictionaries
        """
        cars = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
                )
                
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    locale='de-AT'
                )
                
                page = context.new_page()
                
                logger.info(f"Navigating to {self.BASE_URL}")
                page.goto(self.BASE_URL, wait_until="domcontentloaded", timeout=60000)

                # Handle cookie consent - try multiple selectors
                try:
                    page.wait_for_timeout(2000)
                    cookie_selectors = [
                        'button#didomi-notice-agree-button',
                        'button[data-testid="uc-accept-all-button"]',
                        'button:has-text("Akzeptieren")',
                        'button:has-text("Alle akzeptieren")'
                    ]
                    for selector in cookie_selectors:
                        try:
                            btn = page.query_selector(selector)
                            if btn and btn.is_visible():
                                btn.click()
                                page.wait_for_timeout(1000)
                                logger.info(f"Accepted cookies using selector: {selector}")
                                break
                        except:
                            continue
                except Exception as e:
                    logger.info(f"No cookie dialog or already accepted: {e}")

                # Wait for page to fully load - reduced for speed
                page.wait_for_timeout(3000)  # Wait 3 seconds for initial load
                
                # Scroll to trigger lazy loading - reduced for speed
                logger.info("Scrolling to load content...")
                for i in range(2):  # Minimal scrolling for speed
                    page.evaluate("window.scrollBy(0, window.innerHeight)")
                    page.wait_for_timeout(1000)

                # Try multiple strategies to find car listings
                logger.info("Looking for car listings...")
                
                # Strategy 1: Find all links containing /gebrauchtwagen/
                all_car_links = page.query_selector_all('a[href*="/gebrauchtwagen/"]')
                logger.info(f"Strategy 1: Found {len(all_car_links)} links with /gebrauchtwagen/")
                
                # Strategy 2: Find article elements
                articles = page.query_selector_all('article')
                logger.info(f"Strategy 2: Found {len(articles)} article elements")
                
                # Strategy 3: Find any divs/sections that might contain listings
                potential_containers = page.query_selector_all('[class*="ResultList"], [class*="SearchResult"], [data-testid*="result"]')
                logger.info(f"Strategy 3: Found {len(potential_containers)} potential result containers")
                
                # Extract unique car listings
                car_listings = []
                seen_ids = set()
                
                # Process links from Strategy 1
                for link in all_car_links:
                    try:
                        href = link.get_attribute('href')
                        if not href:
                            continue

                        # Build full URL
                        if not href.startswith('http'):
                            full_url = f"https://www.willhaben.at{href}"
                        else:
                            full_url = href

                        # Extract numeric ID from URL
                        # Patterns: /auto/bmw-123456789 or ?adId=123456789
                        id_match = re.search(r'[-/](\d{6,})(?:[/?]|$)', href)
                        if not id_match:
                            id_match = re.search(r'(?:adId|insertId|entryId)=(\d+)', href)
                        
                        if not id_match:
                            continue

                        listing_id = id_match.group(1)

                        # Skip if we've seen this ID or if it's not a car detail page
                        if listing_id in seen_ids:
                            continue
                        
                        # Make sure it's actually a car listing page, not category/search page
                        if '/gebrauchtwagenboerse' in href or '/kategorie' in href:
                            continue
                            
                        seen_ids.add(listing_id)
                        car_listings.append({
                            'link_element': link,
                            'url': full_url,
                            'listing_id': listing_id
                        })
                        
                    except Exception as e:
                        logger.debug(f"Error processing link: {str(e)}")
                        continue

                logger.info(f"Found {len(car_listings)} unique car listings")
                
                if len(car_listings) == 0:
                    logger.warning("No car listings found! Saving debug screenshot...")
                    try:
                        page.screenshot(path="/tmp/debug_screenshot.png")
                        # Also save HTML for debugging
                        html_content = page.content()
                        with open("/tmp/debug_page.html", "w", encoding="utf-8") as f:
                            f.write(html_content)
                        logger.info("Debug files saved: /tmp/debug_screenshot.png and /tmp/debug_page.html")
                    except:
                        pass
                    browser.close()
                    return cars
                
                # Show first few examples
                for i, listing in enumerate(car_listings[:3]):
                    logger.info(f"Example listing {i+1}: {listing['url']}")
                
                # Process each car listing
                for idx, listing_data in enumerate(car_listings[:self.max_cars]):
                    try:
                        link_element = listing_data['link_element']
                        url = listing_data['url']
                        listing_id = listing_data['listing_id']
                        
                        # Get text content
                        try:
                            # Try to get the parent article/container for full info
                            parent_handle = link_element.evaluate_handle(
                                'el => el.closest("article") || el.closest("[class*=\'Card\']") || el.closest("[class*=\'Item\']") || el.parentElement.parentElement'
                            )
                            parent = parent_handle.as_element()
                            text_content = parent.inner_text() if parent else link_element.inner_text()
                        except:
                            text_content = link_element.inner_text()
                        
                        # Extract title
                        link_text = link_element.inner_text().strip()
                        title = link_text if len(link_text) > 5 else text_content.split('\n')[0]
                        title = title[:500]
                        
                        if not title or len(title) < 3:
                            title = f"Car Listing {listing_id}"
                        
                        # Extract thumbnail only for speed
                        image_url = None
                        try:
                            img = link_element.query_selector('img')
                            if not img and parent:
                                img = parent.query_selector('img')
                            if img:
                                image_url = (img.get_attribute('src') or 
                                           img.get_attribute('data-src') or
                                           img.get_attribute('data-lazy-src') or
                                           img.get_attribute('srcset', '').split()[0] if img.get_attribute('srcset') else None)
                        except:
                            pass
                        
                        # Store as array for consistency
                        image_urls = [image_url] if image_url else []

                        # Initialize variables to avoid undefined errors
                        price = self._extract_price(text_content)
                        year = self._extract_year(text_content)
                        mileage = self._extract_mileage(text_content)
                        location = self._extract_location(text_content)
                        posted_at = self._extract_posted_date(text_content)
                        brand, model = self._parse_brand_model(title)

                        car_data = {
                            'listing_id': listing_id,
                            'title': title,
                            'price': price,
                            'currency': 'EUR',
                            'brand': brand,
                            'model': model,
                            'year': year,
                            'mileage': mileage,
                            'fuel_type': None,
                            'transmission': None,
                            'location': location,
                            'image_urls': image_urls,  # Array instead of single URL
                            'url': url,
                            'description': text_content[:500] if text_content else title,
                            'posted_at': posted_at,  # When car was posted on Willhaben
                        }
                        
                        cars.append(car_data)
                        logger.info(f"✓ {idx + 1}/{min(len(car_listings), self.max_cars)}: {title[:50]}... €{price or '?'}")
                        
                    except Exception as e:
                        logger.error(f"✗ Error extracting car {idx + 1}: {str(e)}")
                        continue
                
                browser.close()
                logger.info(f"Scraping completed: {len(cars)} cars extracted")
                
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        return cars
    
    def _extract_price(self, text: str) -> Optional[float]:
        """Extract price from text"""
        price_patterns = [r'€\s*([\d.,]+)', r'([\d.,]+)\s*€']
        for pattern in price_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    price_str = match.group(1).replace('.', '').replace(',', '.')
                    return float(price_str)
                except:
                    pass
        return None
    
    def _extract_year(self, text: str) -> Optional[int]:
        """Extract year from text"""
        match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', text)
        if match:
            try:
                year = int(match.group(0))
                if 1990 <= year <= 2025:
                    return year
            except:
                pass
        return None
    
    def _extract_mileage(self, text: str) -> Optional[int]:
        """Extract mileage from text"""
        match = re.search(r'([\d.]+)\s*km', text, re.IGNORECASE)
        if match:
            try:
                mileage_str = match.group(1).replace('.', '')
                return int(mileage_str)
            except:
                pass
        return None
    
    def _extract_location(self, text: str) -> Optional[str]:
        """Extract location from text"""
        match = re.search(r'\b(\d{4}\s+[A-ZÄÖÜa-zäöüß\s-]+?)(?:\n|$)', text)
        if match:
            return match.group(1).strip()[:200]
        return None
    
    def _extract_posted_date(self, text: str) -> Optional[datetime]:
        """Extract posting date/time from text"""
        try:
            # Look for relative time patterns (e.g., "vor 5 Minuten", "vor 2 Stunden", "Heute", "Gestern")
            if 'vor' in text.lower():
                # "vor X Minuten"
                match = re.search(r'vor\s+(\d+)\s+minute[n]?', text, re.IGNORECASE)
                if match:
                    minutes = int(match.group(1))
                    return datetime.utcnow() - timedelta(minutes=minutes)
                
                # "vor X Stunden"
                match = re.search(r'vor\s+(\d+)\s+stunde[n]?', text, re.IGNORECASE)
                if match:
                    hours = int(match.group(1))
                    return datetime.utcnow() - timedelta(hours=hours)
                
                # "vor X Tagen"
                match = re.search(r'vor\s+(\d+)\s+tag[en]*', text, re.IGNORECASE)
                if match:
                    days = int(match.group(1))
                    return datetime.utcnow() - timedelta(days=days)
            
            # "Heute" means today
            if 'heute' in text.lower():
                return datetime.utcnow()
            
            # "Gestern" means yesterday
            if 'gestern' in text.lower():
                return datetime.utcnow() - timedelta(days=1)
            
            # Look for specific date patterns (e.g., "03.11.2025")
            match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
            if match:
                day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                return datetime(year, month, day)
                
        except Exception as e:
            logger.debug(f"Error parsing posted date: {e}")
        
        return None
    
    def _extract_posted_time(self, text: str) -> Optional[datetime]:
        """Extract when the listing was posted"""
        now = datetime.utcnow()
        
        # Check for relative time (e.g., "vor 2 Stunden", "vor 30 Minuten", "Heute", "Gestern")
        if 'vor' in text.lower():
            # "vor X Minuten"
            match = re.search(r'vor\s+(\d+)\s*min', text, re.IGNORECASE)
            if match:
                minutes = int(match.group(1))
                return now - timedelta(minutes=minutes)
            
            # "vor X Stunden"
            match = re.search(r'vor\s+(\d+)\s*stunde', text, re.IGNORECASE)
            if match:
                hours = int(match.group(1))
                return now - timedelta(hours=hours)
            
            # "vor X Tagen"
            match = re.search(r'vor\s+(\d+)\s*tag', text, re.IGNORECASE)
            if match:
                days = int(match.group(1))
                return now - timedelta(days=days)
        
        # Check for "Heute" (Today)
        if 'heute' in text.lower():
            return now
        
        # Check for "Gestern" (Yesterday)
        if 'gestern' in text.lower():
            return now - timedelta(days=1)
        
        # Try to find specific date format (e.g., "20.10.2024")
        match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
        if match:
            try:
                day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                return datetime(year, month, day)
            except:
                pass
        
        return None
    
    def _parse_brand_model(self, title: str) -> tuple:
        """Parse brand and model from title"""
        common_brands = [
            'Abarth', 'Alfa Romeo', 'Aston Martin', 'Audi', 'Bentley', 'BMW', 'Bugatti',
            'Cadillac', 'Chevrolet', 'Chrysler', 'Citroën', 'Citroen', 'Cupra', 'Dacia',
            'Dodge', 'Ferrari', 'Fiat', 'Ford', 'Honda', 'Hummer', 'Hyundai', 'Infiniti',
            'Jaguar', 'Jeep', 'Kia', 'Lamborghini', 'Lancia', 'Land Rover', 'Lexus',
            'Maserati', 'Mazda', 'McLaren', 'Mercedes-Benz', 'Mercedes', 'MG', 'Mini',
            'Mitsubishi', 'Nissan', 'Opel', 'Peugeot', 'Porsche', 'Renault', 'Rolls-Royce',
            'Saab', 'Seat', 'Skoda', 'Smart', 'Subaru', 'Suzuki', 'Tesla', 'Toyota',
            'Volkswagen', 'VW', 'Volvo'
        ]
        
        title_upper = title.upper()
        
        for brand in common_brands:
            if brand.upper() in title_upper:
                pattern = re.compile(rf'\b{re.escape(brand)}\b', re.IGNORECASE)
                match = pattern.search(title)
                
                if match:
                    after_brand = title[match.end():].strip()
                    model_match = re.match(r'^[\s\-]*([A-Za-z0-9\-]+(?:\s+[A-Za-z0-9\-]+)?)', after_brand)
                    if model_match:
                        model = model_match.group(1).strip()
                        model = re.sub(r'[^\w\s\-]', '', model).strip()
                        if model and len(model) > 1:
                            return brand, model
                
                return brand, None
        
        return None, None
    
    def scrape_car_images(self, page, car_url: str) -> List[str]:
        """
        Visit car detail page and extract all images from gallery
        """
        images = []
        
        try:
            logger.info(f"Fetching images from detail page: {car_url}")
            page.goto(car_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)
            
            # Try multiple selectors for image galleries
            image_selectors = [
                'img[class*="gallery"]',
                '[class*="ImageGallery"] img',
                '[class*="Carousel"] img',
                '[data-testid*="image"] img',
                'picture img',
                '.image-gallery img'
            ]
            
            seen_urls = set()
            
            for selector in image_selectors:
                img_elements = page.query_selector_all(selector)
                for img in img_elements:
                    url = (
                        img.get_attribute('src') or 
                        img.get_attribute('data-src') or
                        img.get_attribute('data-original')
                    )
                    
                    # Handle srcset
                    if not url:
                        srcset = img.get_attribute('srcset')
                        if srcset:
                            # Get highest resolution image
                            urls = [s.strip().split()[0] for s in srcset.split(',')]
                            if urls:
                                url = urls[-1]  # Last one is usually highest res
                    
                    if url and url not in seen_urls:
                        # Fix relative URLs
                        if url.startswith('//'):
                            url = f"https:{url}"
                        elif url.startswith('/'):
                            url = f"https://www.willhaben.at{url}"
                        
                        # Skip thumbnails and icons
                        if 'thumb' not in url.lower() and 'icon' not in url.lower() and not url.endswith('.svg'):
                            images.append(url)
                            seen_urls.add(url)
            
            logger.info(f"Found {len(images)} images for car")
            return images[:10]  # Limit to 10 images max
            
        except Exception as e:
            logger.error(f"Error scraping images from {car_url}: {str(e)}")
            return []


# ============================================================================
# BACKGROUND JOBS
# ============================================================================

def scrape_and_store_cars():
    """Fast scraping job - thumbnails only for speed"""
    with app.app_context():
        log_entry = ScrapingLog()
        db.session.add(log_entry)
        db.session.commit()
        
        try:
            logger.info("Starting FAST scraping job (thumbnails only)...")
            
            # Fast scraping - thumbnails only, maximum cars
            scraper = WillhabenScraper(max_cars=9999, full_image_scraping=False)
            scraped_cars = scraper.scrape_listings()
            
            log_entry.cars_found = len(scraped_cars)
            cars_added = 0
            cars_updated = 0
            
            # Get all current listing IDs to mark inactive
            current_listing_ids = {car['listing_id'] for car in scraped_cars}
            
            for car_data in scraped_cars:
                existing_car = Car.query.filter_by(listing_id=car_data['listing_id']).first()
                
                if existing_car:
                    # Update existing car
                    existing_car.last_seen_at = datetime.utcnow()
                    existing_car.is_active = True
                    existing_car.price = car_data.get('price')
                    existing_car.updated_at = datetime.utcnow()
                    # Update posted_at if we have new data
                    if car_data.get('posted_at'):
                        existing_car.posted_at = car_data.get('posted_at')
                    cars_updated += 1
                else:
                    # Add new car
                    new_car = Car(**car_data)
                    db.session.add(new_car)
                    cars_added += 1
                    logger.info(f"🆕 NEW CAR: {car_data.get('title', 'Unknown')} - Posted: {car_data.get('posted_at', 'Unknown')}")
            
            # Mark cars as inactive if not seen in this scrape
            if current_listing_ids and len(scraped_cars) > 10:  # Safeguard: Only deactivate if >10 cars scraped
                inactive_count = Car.query.filter(
                    and_(
                        Car.listing_id.notin_(current_listing_ids),
                        Car.is_active == True
                    )
                ).update({'is_active': False}, synchronize_session=False)
                logger.info(f"Marked {inactive_count} cars as inactive")
            else:
                logger.warning("Skipping deactivation: Too few cars scraped or scrape failed")
            
            db.session.commit()
            
            log_entry.scrape_completed_at = datetime.utcnow()
            log_entry.cars_added = cars_added
            log_entry.cars_updated = cars_updated
            log_entry.status = 'success'
            db.session.commit()
            
            logger.info(f"Scraping completed: {cars_added} added, {cars_updated} updated, {len(scraped_cars)} total")
            
        except Exception as e:
            logger.error(f"Scraping job failed: {str(e)}")
            log_entry.status = 'failed'
            log_entry.error_message = str(e)
            log_entry.scrape_completed_at = datetime.utcnow()
            db.session.commit()


def enrich_cars_with_images():
    """Background job to enrich cars with full image galleries"""
    with app.app_context():
        try:
            logger.info("Starting image enrichment job...")
            
            # Find cars that only have 1 or 0 images (thumbnails only)
            cars_needing_images = Car.query.filter(
                and_(
                    Car.is_active == True,
                    or_(
                        Car.image_urls == None,
                        func.jsonb_array_length(Car.image_urls) <= 1
                    )
                )
            ).order_by(Car.first_seen_at.desc()).limit(20).all()  # Process 20 cars per run
            
            if not cars_needing_images:
                logger.info("No cars need image enrichment")
                return
            
            logger.info(f"Found {len(cars_needing_images)} cars needing full images")
            
            # Use Playwright to visit detail pages
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
                )
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    locale='de-AT'
                )
                page = context.new_page()
                
                scraper = WillhabenScraper(max_cars=1, full_image_scraping=False)
                enriched_count = 0
                
                for car in cars_needing_images:
                    try:
                        logger.info(f"Enriching images for: {car.title[:50]}...")
                        
                        # Get full images from detail page
                        full_images = scraper.scrape_car_images(page, car.url)
                        
                        if full_images and len(full_images) > 1:
                            car.image_urls = full_images
                            car.updated_at = datetime.utcnow()
                            enriched_count += 1
                            logger.info(f"✓ Added {len(full_images)} images to {car.listing_id}")
                        else:
                            logger.debug(f"No additional images found for {car.listing_id}")
                        
                    except Exception as e:
                        logger.error(f"Error enriching car {car.listing_id}: {str(e)}")
                        continue
                
                browser.close()
            
            db.session.commit()
            logger.info(f"Image enrichment completed: {enriched_count}/{len(cars_needing_images)} cars enriched")
            
        except Exception as e:
            logger.error(f"Image enrichment job failed: {str(e)}")
            db.session.rollback()


def cleanup_inactive_cars():
    """Daily cleanup job to remove old inactive cars"""
    with app.app_context():
        try:
            logger.info("Starting daily cleanup job...")
            
            # Remove cars that have been inactive for more than 7 days
            cutoff_date = datetime.utcnow() - timedelta(days=7)
            deleted_count = Car.query.filter(
                and_(
                    Car.is_active == False,
                    Car.last_seen_at < cutoff_date
                )
            ).delete()
            
            db.session.commit()
            logger.info(f"Cleanup completed: {deleted_count} cars removed")
            
        except Exception as e:
            logger.error(f"Cleanup job failed: {str(e)}")
            db.session.rollback()


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        db.session.execute(text('SELECT 1'))
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e)
        }), 500


@app.route('/api/cars', methods=['GET'])
def get_cars():
    """Get paginated list of cars - sorted by most recent first"""
    try:
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 20, type=int)
        limit = min(limit, 100)
        
        # Sort by posted_at (when car was uploaded to Willhaben), then last_seen_at
        query = Car.query.filter_by(is_active=True).order_by(
            Car.posted_at.desc().nulls_last(), 
            Car.last_seen_at.desc(), 
            Car.first_seen_at.desc()
        )
        pagination = query.paginate(page=page, per_page=limit, error_out=False)
        
        return jsonify({
            'cars': [car.to_dict() for car in pagination.items],
            'pagination': {
                'page': page,
                'limit': limit,
                'total': pagination.total,
                'pages': pagination.pages,
                'has_next': pagination.has_next,
                'has_prev': pagination.has_prev
            }
        }), 200
    except Exception as e:
        logger.error(f"Error in get_cars: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cars/<listing_id>', methods=['GET'])
def get_car(listing_id):
    """Get single car by listing ID"""
    try:
        car = Car.query.filter_by(listing_id=listing_id, is_active=True).first()
        if not car:
            return jsonify({'error': 'Car not found'}), 404
        return jsonify({'car': car.to_dict()}), 200
    except Exception as e:
        logger.error(f"Error in get_car: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cars/search', methods=['GET'])
def search_cars():
    """Search cars with filters"""
    try:
        brand = request.args.get('brand')
        model = request.args.get('model')
        min_price = request.args.get('min_price', type=float)
        max_price = request.args.get('max_price', type=float)
        min_year = request.args.get('min_year', type=int)
        max_year = request.args.get('max_year', type=int)
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 20, type=int)
        
        query = Car.query.filter_by(is_active=True)
        
        if brand:
            query = query.filter(Car.brand.ilike(f'%{brand}%'))
        if model:
            query = query.filter(Car.model.ilike(f'%{model}%'))
        if min_price is not None:
            query = query.filter(Car.price >= min_price)
        if max_price is not None:
            query = query.filter(Car.price <= max_price)
        if min_year is not None:
            query = query.filter(Car.year >= min_year)
        if max_year is not None:
            query = query.filter(Car.year <= max_year)
        
        query = query.order_by(Car.first_seen_at.desc())
        limit = min(limit, 100)
        pagination = query.paginate(page=page, per_page=limit, error_out=False)
        
        return jsonify({
            'cars': [car.to_dict() for car in pagination.items],
            'filters': {
                'brand': brand,
                'model': model,
                'min_price': min_price,
                'max_price': max_price,
                'min_year': min_year,
                'max_year': max_year
            },
            'pagination': {
                'page': page,
                'limit': limit,
                'total': pagination.total,
                'pages': pagination.pages,
                'has_next': pagination.has_next,
                'has_prev': pagination.has_prev
            }
        }), 200
    except Exception as e:
        logger.error(f"Error in search_cars: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cars/latest', methods=['GET'])
def get_latest_car():
    """Get the single most recent car uploaded"""
    try:
        # Get the most recently posted car (by Willhaben upload time)
        latest_car = Car.query.filter_by(is_active=True).order_by(
            Car.posted_at.desc().nulls_last(),
            Car.last_seen_at.desc(), 
            Car.first_seen_at.desc()
        ).first()
        
        if not latest_car:
            return jsonify({'error': 'No cars found'}), 404
        
        return jsonify({
            'car': latest_car.to_dict(),
            'timestamp': datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error in get_latest_car: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cars/recent', methods=['GET'])
def get_recent_cars():
    """Get most recently seen cars (within last 24 hours or most recent)"""
    try:
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        limit = request.args.get('limit', 20, type=int)
        limit = min(limit, 100)
        
        # Sort by posted_at to show the most recently uploaded cars on Willhaben
        cars = Car.query.filter(
            and_(
                Car.is_active == True,
                Car.first_seen_at >= cutoff_time
            )
        ).order_by(
            Car.posted_at.desc().nulls_last(),
            Car.last_seen_at.desc(), 
            Car.first_seen_at.desc()
        ).limit(limit).all()
        
        return jsonify({
            'cars': [car.to_dict() for car in cars],
            'count': len(cars),
            'cutoff_time': cutoff_time.isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error in get_recent_cars: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get scraping statistics"""
    try:
        total_cars = Car.query.filter_by(is_active=True).count()
        total_brands = db.session.query(func.count(func.distinct(Car.brand))).scalar()
        recent_scrape = ScrapingLog.query.order_by(ScrapingLog.scrape_started_at.desc()).first()
        
        stats = {
            'total_active_cars': total_cars,
            'total_brands': total_brands,
            'last_scrape': recent_scrape.scrape_started_at.isoformat() if recent_scrape else None,
            'last_scrape_status': recent_scrape.status if recent_scrape else None,
            'last_scrape_cars_found': recent_scrape.cars_found if recent_scrape else 0
        }
        
        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"Error in get_stats: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/trigger-scrape', methods=['POST'])
def trigger_scrape():
    """Manual trigger for scraping"""
    try:
        scrape_and_store_cars()
        return jsonify({'message': 'Scraping job triggered successfully'}), 200
    except Exception as e:
        logger.error(f"Error triggering scrape: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# SCHEDULER SETUP
# ============================================================================

def init_scheduler():
    """Initialize APScheduler with background jobs"""
    scheduler = BackgroundScheduler(timezone='UTC')
    
    # STAGE 1: Fast scraping - thumbnails only (every 5 seconds)
    scheduler.add_job(
        func=scrape_and_store_cars,
        trigger=IntervalTrigger(seconds=5),
        id='fast_scrape_job',
        name='Fast scrape (thumbnails) every 5 seconds',
        replace_existing=True
    )
    
    # STAGE 2: Image enrichment - full galleries (every 2 minutes)
    scheduler.add_job(
        func=enrich_cars_with_images,
        trigger=IntervalTrigger(minutes=2),
        id='image_enrichment_job',
        name='Enrich cars with full images every 2 minutes',
        replace_existing=True
    )
    
    # STAGE 3: Daily cleanup
    scheduler.add_job(
        func=cleanup_inactive_cars,
        trigger=CronTrigger(hour=23, minute=0),
        id='cleanup_job',
        name='Daily cleanup at 00:00 CET',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Scheduler started")
    atexit.register(lambda: scheduler.shutdown())
    
    return scheduler


# ============================================================================
# APP INITIALIZATION
# ============================================================================

def init_app():
    """Initialize the application"""
    with app.app_context():
        db.create_all()
        logger.info("Database tables created")
        
        # Run migration to add posted_at column if it doesn't exist
        try:
            db.session.execute(text("""
                ALTER TABLE cars 
                ADD COLUMN IF NOT EXISTS posted_at TIMESTAMP
            """))
            db.session.commit()
            logger.info("Database migration: posted_at column added/verified")
        except Exception as e:
            logger.warning(f"Migration may have already run or failed: {e}")
            db.session.rollback()
        
        car_count = Car.query.count()
        if car_count == 0:
            logger.info("No cars in database, running initial scrape...")
            try:
                scrape_and_store_cars()
            except Exception as e:
                logger.error(f"Initial scrape failed: {str(e)}")


# Initialize on startup
if __name__ != '__main__':
    init_app()
    scheduler = init_scheduler()

if __name__ == '__main__':
    init_app()
    scheduler = init_scheduler()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)), debug=False)