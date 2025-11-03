# app.py
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import pytz
import requests

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, and_, func
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
    image_url = db.Column(db.Text)
    url = db.Column(db.Text, nullable=False)
    description = db.Column(db.Text)
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
            'image_url': self.image_url,
            'url': self.url,
            'description': self.description,
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
    """Scraper for willhaben.at car listings"""
    
    BASE_URL = "https://www.willhaben.at/iad/gebrauchtwagen/auto/gebrauchtwagenboerse"
    
    def __init__(self, max_cars: int = 100):
        self.max_cars = max_cars
    
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
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                
                page = context.new_page()
                
                logger.info(f"Navigating to {self.BASE_URL}")
                page.goto(self.BASE_URL, wait_until="networkidle", timeout=60000)

                # Accept cookies
                try:
                    btn = page.query_selector('button#didomi-notice-agree-button, button[data-testid="uc-accept-all-button"]')
                    if btn:
                        btn.click()
                        page.wait_for_timeout(1000)
                        logger.info("Accepted cookie consent dialog")
                except Exception:
                    pass

                # Wait for the results container
                try:
                    page.wait_for_selector('[data-testid="result-list"]', timeout=15000)
                    logger.info("Result list container loaded")
                except PlaywrightTimeout:
                    logger.warning("Result list container did not appear in time")

                # Dump the rendered HTML for inspection
                html = page.content()
                with open("/tmp/debug.html", "w", encoding="utf-8") as f:
                    f.write(html)
                logger.info("Dumped page HTML to /tmp/debug.html")

                # Now query for listings
                listing_elements = page.query_selector_all('[data-testid="result-list"] article')
                logger.info(f"Found {len(listing_elements)} listing <article> elements")

                car_listings = []
                seen_ids = set()
                import re

                for item in listing_elements:
                    try:
                        link = item.query_selector('a[href*="/iad/gebrauchtwagen/"]')
                        if not link:
                            continue

                        href = link.get_attribute('href')
                        if not href:
                            continue

                        if not href.startswith('http'):
                            href = f"https://www.willhaben.at{href}"

                        # Extract numeric ID
                        match = re.search(r'[-/](\d{6,})(?:\?|$)', href)
                        if not match:
                            # Try fallback for redirect or query parameter formats
                            match = re.search(r'(?:adId|insertId|entryId)=(\d+)', href)
                        if not match:
                            continue

                        listing_id = match.group(1)

                        if listing_id not in seen_ids:
                            seen_ids.add(listing_id)
                            car_listings.append({
                                'link_element': link,
                                'url': href,
                                'listing_id': listing_id
                            })
                    except Exception as e:
                        logger.debug(f"Error processing listing: {str(e)}")
                        continue

                logger.info(f"Found {len(car_listings)} unique car listings")
                if car_listings:
                    logger.info(f"Example listing: {car_listings[0]['url']}")
                
                logger.info(f"Found {len(car_listings)} unique car listings")
                
                if len(car_listings) == 0:
                    logger.warning("No car listings found! Page might not have loaded properly.")
                    browser.close()
                    return cars
                
                # Process each car listing
                for idx, listing_data in enumerate(car_listings[:self.max_cars]):
                    try:
                        link_element = listing_data['link_element']
                        url = listing_data['url']
                        listing_id = listing_data['listing_id']
                        
                        # Get text content from the link and its parent container
                        link_text = link_element.inner_text().strip()
                        
                        # Try to get parent container with more info
                        try:
                            # Go up the DOM tree to find the card container
                            parent = link_element.evaluate_handle('el => el.closest("article, [class*=\"Card\"], [class*=\"card\"], [class*=\"Item\"], [class*=\"item\"])')
                            parent_element = parent.as_element() if parent else None
                            
                            if parent_element:
                                text_content = parent_element.inner_text()
                            else:
                                # Fallback: go up 3 levels
                                parent_el = link_element
                                for _ in range(3):
                                    parent_handle = parent_el.evaluate_handle('el => el.parentElement')
                                    parent_el = parent_handle.as_element()
                                    if not parent_el:
                                        break
                                text_content = parent_el.inner_text() if parent_el else link_text
                        except:
                            text_content = link_text
                        
                        # Use link text as title, fallback to first line of content
                        title = link_text if link_text and len(link_text) > 5 else text_content.split('\n')[0]
                        title = title[:500]  # Limit length
                        
                        # Extract image - try multiple methods
                        image_url = None
                        try:
                            # Try to find img near the link
                            img_handle = link_element.evaluate_handle('''el => {
                                return el.querySelector('img') || 
                                       el.parentElement?.querySelector('img') ||
                                       el.parentElement?.parentElement?.querySelector('img');
                            }''')
                            img_element = img_handle.as_element() if img_handle else None
                            
                            if img_element:
                                image_url = (img_element.get_attribute('src') or 
                                           img_element.get_attribute('data-src') or
                                           img_element.get_attribute('data-lazy-src'))
                        except:
                            pass
                        
                        # Parse price from text
                        price = None
                        import re
                        # Match patterns like "€ 15.900" or "15.900 €" or "€15900"
                        price_patterns = [
                            r'€\s*([\d.,]+)',
                            r'([\d.,]+)\s*€',
                        ]
                        for pattern in price_patterns:
                            price_match = re.search(pattern, text_content)
                            if price_match:
                                try:
                                    price_str = price_match.group(1).replace('.', '').replace(',', '.')
                                    price = float(price_str)
                                    break
                                except:
                                    pass
                        
                        # Parse year (4-digit number starting with 19 or 20)
                        year = None
                        year_match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', text_content)
                        if year_match:
                            try:
                                year_candidate = int(year_match.group(0))
                                if 1990 <= year_candidate <= 2025:
                                    year = year_candidate
                            except:
                                pass
                        
                        # Parse mileage (look for numbers followed by km)
                        mileage = None
                        mileage_match = re.search(r'([\d.]+)\s*km', text_content, re.IGNORECASE)
                        if mileage_match:
                            try:
                                mileage_str = mileage_match.group(1).replace('.', '')
                                mileage = int(mileage_str)
                            except:
                                pass
                        
                        # Parse location (Austrian postal codes are 4 digits + city name)
                        location = None
                        location_match = re.search(r'\b(\d{4}\s+[A-ZÄÖÜa-zäöüß\s-]+?)(?:\n|$)', text_content)
                        if location_match:
                            location = location_match.group(1).strip()[:200]
                        
                        # Parse brand and model from title
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
                            'image_url': image_url,
                            'url': url,
                            'description': text_content[:500] if text_content else title,
                        }
                        
                        cars.append(car_data)
                        logger.info(f"✓ Scraped {idx + 1}/{min(len(car_listings), self.max_cars)}: {title[:60]}... (€{price or '?'}, {year or '?'})")
                        
                    except Exception as e:
                        logger.error(f"✗ Error extracting car {idx + 1} (ID: {listing_data.get('listing_id', '?')}): {str(e)}")
                        continue
                
                browser.close()
                logger.info(f"Scraping completed successfully: {len(cars)} cars extracted")
                
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        return cars
    
    def scrape_listings_json_api(self, max_cars: int = 100) -> List[Dict[str, Any]]:
        """
        Scrape car listings from Willhaben's internal JSON API
        Returns list of car dictionaries
        """
        # Example endpoint and params (update these after inspecting DevTools)
        api_url = "https://api.willhaben.at/restapi/api/v2/search/advertisement"  # Example, update as needed
        params = {
            "rows": max_cars,
            "page": 1,
            # Add other required params here
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        try:
            response = requests.get(api_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            # Parse car listings from JSON response
            car_listings = []
            for item in data.get("searchResults", []):  # Update key as needed
                car = {
                    "listing_id": item.get("id"),
                    "title": item.get("title"),
                    "price": item.get("price"),
                    "currency": item.get("currency"),
                    "brand": item.get("make"),
                    "model": item.get("model"),
                    "year": item.get("year"),
                    "mileage": item.get("mileage"),
                    "fuel_type": item.get("fuelType"),
                    "transmission": item.get("transmission"),
                    "location": item.get("location"),
                    "image_url": item.get("imageUrl"),
                    "url": item.get("url"),
                    "description": item.get("description"),
                }
                car_listings.append(car)
            logger.info(f"Fetched {len(car_listings)} cars from JSON API")
            return car_listings
        except Exception as e:
            logger.error(f"Failed to fetch from JSON API: {str(e)}")
            return []
    
    def _parse_brand_model(self, title: str) -> tuple:
        """Enhanced brand/model parsing from title"""
        import re
        
        # Extended list of car brands
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
            brand_upper = brand.upper()
            if brand_upper in title_upper:
                # Find the position of the brand in the title
                pattern = re.compile(rf'\b{re.escape(brand)}\b', re.IGNORECASE)
                match = pattern.search(title)
                
                if match:
                    # Get text after the brand name
                    after_brand = title[match.end():].strip()
                    
                    # Extract model (first word/phrase after brand)
                    model_match = re.match(r'^[\s\-]*([A-Za-z0-9\-]+(?:\s+[A-Za-z0-9\-]+)?)', after_brand)
                    if model_match:
                        model = model_match.group(1).strip()
                        # Clean up model
                        model = re.sub(r'[^\w\s\-]', '', model).strip()
                        if model and len(model) > 1:
                            return brand, model
                
                return brand, None
            
            
        
        return None, None
    
    


# ============================================================================
# BACKGROUND JOBS
# ============================================================================

def scrape_and_store_cars():
    """Background job to scrape cars and store in database"""
    with app.app_context():
        log_entry = ScrapingLog()
        db.session.add(log_entry)
        db.session.commit()
        
        try:
            logger.info("Starting background scraping job...")
            
            scraper = WillhabenScraper(max_cars=100)
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
                    cars_updated += 1
                else:
                    # Add new car
                    new_car = Car(**car_data)
                    db.session.add(new_car)
                    cars_added += 1
            
            # Mark cars as inactive if not seen in this scrape
            if current_listing_ids:
                Car.query.filter(
                    and_(
                        Car.listing_id.notin_(current_listing_ids),
                        Car.is_active == True
                    )
                ).update({'is_active': False}, synchronize_session=False)
            
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
@app.route('/api/debug-links', methods=['GET'])
def debug_links():
    """Debug endpoint to see actual links found"""
    try:
        from playwright.sync_api import sync_playwright
        
        results = {
            'status': 'running',
            'all_links': [],
            'car_links': [],
            'errors': []
        }
        
        url = request.args.get('url', 'https://www.willhaben.at/iad/kaufen-und-verkaufen/auto')
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            # Navigate
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            page.wait_for_timeout(7000)
            
            # Scroll
            page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            page.wait_for_timeout(2000)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            
            # Get all links
            all_links = page.query_selector_all('a[href*="/iad/"]')
            
            # Extract all hrefs
            for link in all_links:
                href = link.get_attribute('href')
                if href:
                    link_text = link.inner_text().strip()[:100]
                    results['all_links'].append({
                        'href': href,
                        'text': link_text
                    })
            
            # Filter car links
            seen_ids = set()
            for link_data in results['all_links']:
                href = link_data['href']
                parts = href.split('/')
                
                if 'auto' in href:
                    potential_id = parts[-1].split('?')[0]
                    if potential_id.isdigit() and potential_id not in seen_ids:
                        seen_ids.add(potential_id)
                        results['car_links'].append({
                            'id': potential_id,
                            'href': href,
                            'text': link_data['text']
                        })
            
            browser.close()
        
        results['status'] = 'completed'
        results['total_links'] = len(results['all_links'])
        results['total_car_links'] = len(results['car_links'])
        
        return jsonify(results), 200
        
    except Exception as e:
        return jsonify({
            'status': 'failed',
            'error': str(e)
        }), 500
     
@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Check database connection - fixed SQLAlchemy syntax
        from sqlalchemy import text
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
    """Get paginated list of cars"""
    try:
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 20, type=int)
        
        # Limit max results per page
        limit = min(limit, 100)
        
        # Query active cars
        query = Car.query.filter_by(is_active=True).order_by(Car.first_seen_at.desc())
        
        # Paginate
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
        # Get query parameters
        brand = request.args.get('brand')
        model = request.args.get('model')
        min_price = request.args.get('min_price', type=float)
        max_price = request.args.get('max_price', type=float)
        min_year = request.args.get('min_year', type=int)
        max_year = request.args.get('max_year', type=int)
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 20, type=int)
        
        # Build query
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
        
        # Paginate
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


@app.route('/api/cars/recent', methods=['GET'])
def get_recent_cars():
    """Get cars added in the last 24 hours"""
    try:
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        limit = request.args.get('limit', 20, type=int)
        limit = min(limit, 100)
        
        cars = Car.query.filter(
            and_(
                Car.is_active == True,
                Car.first_seen_at >= cutoff_time
            )
        ).order_by(Car.first_seen_at.desc()).limit(limit).all()
        
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
    """Manual trigger for scraping (useful for testing)"""
    try:
        scrape_and_store_cars()
        return jsonify({'message': 'Scraping job triggered successfully'}), 200
    except Exception as e:
        logger.error(f"Error triggering scrape: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-scrape', methods=['GET'])
def test_scrape():
    """Test scraping endpoint with detailed logging"""
    try:
        from playwright.sync_api import sync_playwright
        
        results = {
            'status': 'running',
            'steps': [],
            'errors': []
        }
        
        # Get custom URL from query param or use default
        url = request.args.get('url', 'https://www.willhaben.at/iad/kaufen-und-verkaufen/auto')
        results['url'] = url
        
        with sync_playwright() as p:
            results['steps'].append('Playwright started')
            
            browser = p.chromium.launch(headless=True)
            results['steps'].append('Browser launched')
            
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            results['steps'].append('Page created')
            
            # Navigate
            results['steps'].append(f'Navigating to {url}')
            response = page.goto(url, wait_until='networkidle', timeout=60000)
            results['steps'].append(f'Navigation complete - Status: {response.status}')
            
            # Wait a bit
            page.wait_for_timeout(10000)
            results['steps'].append('Waited 3 seconds')
            
            # Get page title
            title = page.title()
            results['page_title'] = title
            results['steps'].append(f'Page title: {title}')
            
            # Try to find listings with different selectors
            selectors_to_try = [
                '[data-testid="search-result-entry"]',
                '.search-result-entry',
                'article',
                '[class*="SearchResult"]',
                '[class*="search"]',
                'a[href*="/iad/"]'
            ]
            
            for selector in selectors_to_try:
                count = page.locator(selector).count()
                results['steps'].append(f'Selector "{selector}": {count} elements found')
            
            # Get page content sample (first 500 chars)
            content = page.content()
            results['html_sample'] = content[:500]
            results['html_length'] = len(content)
            
            # Take a screenshot (base64)
            screenshot = page.screenshot()
            import base64
            results['screenshot_base64'] = base64.b64encode(screenshot).decode('utf-8')
            
            browser.close()
            results['steps'].append('Browser closed')
        
        results['status'] = 'completed'
        return jsonify(results), 200
        
    except Exception as e:
        return jsonify({
            'status': 'failed',
            'error': str(e),
            'error_type': type(e).__name__
        }), 500
    

# ============================================================================
# SCHEDULER SETUP
# ============================================================================

def init_scheduler():
    """Initialize APScheduler with background jobs"""
    scheduler = BackgroundScheduler(timezone='UTC')
    
    # Background scraper - every 5 minutes
    scheduler.add_job(
        func=scrape_and_store_cars,
        trigger=IntervalTrigger(minutes=5),
        id='scrape_job',
        name='Scrape cars every 5 minutes',
        replace_existing=True
    )
    
    # Daily cleanup at 00:00 CET (23:00 UTC in winter, 22:00 UTC in summer)
    # Using 23:00 UTC for simplicity
    scheduler.add_job(
        func=cleanup_inactive_cars,
        trigger=CronTrigger(hour=23, minute=0),
        id='cleanup_job',
        name='Daily cleanup at 00:00 CET',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Scheduler started with jobs: scrape_job (every 5 min), cleanup_job (daily at 00:00 CET)")
    
    # Shut down the scheduler when exiting the app
    atexit.register(lambda: scheduler.shutdown())
    
    return scheduler


# ============================================================================
# APP INITIALIZATION
# ============================================================================

def init_app():
    """Initialize the application"""
    with app.app_context():
        # Create tables
        db.create_all()
        logger.info("Database tables created")
        
        # Check if we have any cars, if not run initial scrape
        car_count = Car.query.count()
        if car_count == 0:
            logger.info("No cars in database, running initial scrape...")
            try:
                scrape_and_store_cars()
            except Exception as e:
                logger.error(f"Initial scrape failed: {str(e)}")


# Initialize on startup
if __name__ != '__main__':
    # Running with gunicorn
    init_app()
    scheduler = init_scheduler()


if __name__ == '__main__':
    # Running directly with python
    init_app()
    scheduler = init_scheduler()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)), debug=False)

@app.route("/api/debug-html", methods=["GET"])
def debug_html():
    """Return the dumped Playwright HTML for debugging"""
    import pathlib
    path = pathlib.Path("/tmp/debug.html")
    if path.exists():
        return app.response_class(path.read_text("utf-8"), mimetype="text/html")
    return jsonify({"error": "No debug file found"}), 404