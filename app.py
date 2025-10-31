# app.py
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import pytz

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
    
    BASE_URL = "https://www.willhaben.at/iad/kaufen-und-verkaufen/auto"
    
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
                    args=['--no-sandbox', '--disable-setuid-sandbox']
                )
                
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                
                page = context.new_page()
                
                logger.info(f"Navigating to {self.BASE_URL}")
                page.goto(self.BASE_URL, wait_until='domcontentloaded', timeout=30000)
                
                # Wait longer for JavaScript to render content
                logger.info("Waiting for content to load...")
                page.wait_for_timeout(5000)
                
                # Scroll to trigger lazy loading
                page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                page.wait_for_timeout(2000)
                
                # Find all car listing links
                all_links = page.query_selector_all('a[href*="/iad/"]')
                logger.info(f"Found {len(all_links)} total links")
                
                # Filter to only car listing links (they have specific patterns)
                car_links = []
                seen_ids = set()
                
                for link in all_links:
                    href = link.get_attribute('href')
                    if not href:
                        continue
                    
                    # Car listings typically have numeric IDs at the end
                    # Example: /iad/kaufen-und-verkaufen/auto/bmw-3er-320d/123456789
                    if '/auto/' in href and href.split('/')[-1].isdigit():
                        listing_id = href.split('/')[-1]
                        if listing_id not in seen_ids:
                            car_links.append((link, href, listing_id))
                            seen_ids.add(listing_id)
                
                logger.info(f"Found {len(car_links)} unique car listings")
                
                # Process each car listing
                for idx, (link_element, href, listing_id) in enumerate(car_links[:self.max_cars]):
                    try:
                        # Build full URL
                        if not href.startswith('http'):
                            url = f"https://www.willhaben.at{href}"
                        else:
                            url = href
                        
                        # Get the parent container to extract data
                        parent = link_element
                        for _ in range(5):  # Go up max 5 levels to find the card
                            parent = parent.evaluate_handle('el => el.parentElement').as_element()
                            if not parent:
                                break
                        
                        # Extract text from the card
                        if parent:
                            text_content = parent.inner_text()
                        else:
                            text_content = link_element.inner_text()
                        
                        # Extract title (usually the first line or link text)
                        title = link_element.inner_text().strip()
                        if not title or len(title) < 3:
                            title = text_content.split('\n')[0][:100] if text_content else f"Car Listing {listing_id}"
                        
                        # Extract image
                        image_url = None
                        if parent:
                            img = parent.query_selector('img')
                            if img:
                                image_url = img.get_attribute('src') or img.get_attribute('data-src')
                        
                        # Parse price from text
                        price = None
                        import re
                        price_match = re.search(r'€\s*([\d.,]+)', text_content)
                        if price_match:
                            try:
                                price_str = price_match.group(1).replace('.', '').replace(',', '.')
                                price = float(price_str)
                            except:
                                pass
                        
                        # Parse year
                        year = None
                        year_match = re.search(r'\b(19|20)\d{2}\b', text_content)
                        if year_match:
                            try:
                                year = int(year_match.group(0))
                            except:
                                pass
                        
                        # Parse mileage (km)
                        mileage = None
                        mileage_match = re.search(r'([\d.]+)\s*km', text_content, re.IGNORECASE)
                        if mileage_match:
                            try:
                                mileage_str = mileage_match.group(1).replace('.', '')
                                mileage = int(mileage_str)
                            except:
                                pass
                        
                        # Parse location (usually at the end)
                        location = None
                        lines = text_content.split('\n')
                        for line in reversed(lines):
                            line = line.strip()
                            # Austrian postal codes are 4 digits
                            if re.search(r'\b\d{4}\b', line) and len(line) < 50:
                                location = line
                                break
                        
                        # Parse brand and model from title
                        brand, model = self._parse_brand_model(title)
                        
                        car_data = {
                            'listing_id': listing_id,
                            'title': title[:500],  # Limit title length
                            'price': price,
                            'currency': 'EUR',
                            'brand': brand,
                            'model': model,
                            'year': year,
                            'mileage': mileage,
                            'fuel_type': None,  # Would need detail page scraping
                            'transmission': None,  # Would need detail page scraping
                            'location': location,
                            'image_url': image_url,
                            'url': url,
                            'description': text_content[:500] if text_content else title,
                        }
                        
                        cars.append(car_data)
                        logger.info(f"Scraped car {idx + 1}/{min(len(car_links), self.max_cars)}: {title[:50]}...")
                        
                    except Exception as e:
                        logger.error(f"Error extracting car {idx + 1}: {str(e)}")
                        continue
                
                browser.close()
                logger.info(f"Scraping completed: {len(cars)} cars extracted")
                
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
        
        return cars
    
    def _parse_brand_model(self, title: str) -> tuple:
        """Basic brand/model parsing from title"""
        common_brands = [
            'Abarth', 'Alfa Romeo', 'Audi', 'BMW', 'Chevrolet', 'Citroën', 'Citroen',
            'Cupra', 'Dacia', 'Fiat', 'Ford', 'Honda', 'Hyundai', 'Jaguar',
            'Jeep', 'Kia', 'Land Rover', 'Lexus', 'Mazda', 'Mercedes', 'Mercedes-Benz',
            'Mini', 'Mitsubishi', 'Nissan', 'Opel', 'Peugeot', 'Porsche', 'Renault',
            'Seat', 'Skoda', 'Smart', 'Subaru', 'Suzuki', 'Tesla', 'Toyota',
            'Volkswagen', 'VW', 'Volvo'
        ]
        
        title_upper = title.upper()
        
        for brand in common_brands:
            if brand.upper() in title_upper:
                # Try to extract model (word after brand)
                # Use regex to find the brand and capture what comes after
                pattern = re.compile(rf'\b{re.escape(brand)}\b\s+(\S+)', re.IGNORECASE)
                match = pattern.search(title)
                if match:
                    model = match.group(1)
                    # Clean up model (remove non-alphanumeric except dash)
                    model = re.sub(r'[^\w\s-]', '', model).strip()
                    return brand, model if model else None
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
            response = page.goto(url, wait_until='networkidle', timeout=30000)
            results['steps'].append(f'Navigation complete - Status: {response.status}')
            
            # Wait a bit
            page.wait_for_timeout(3000)
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