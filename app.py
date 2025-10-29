from flask import Flask, jsonify, request
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
import re
from typing import List, Dict, Optional
import json
import os
import shutil
import glob as glob_module
import subprocess

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

class WillhabenCarScraper:
    """
    Streamlined Willhaben scraper - extracts essential car info and ALL images
    """
    def __init__(self):
        self.base_url = "https://www.willhaben.at"
        
    def _create_driver(self, headless=True):
        """Create and configure Chrome WebDriver"""
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument('--headless=new')
        
        # Essential options
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-software-rasterizer')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-setuid-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36')
        
        print("🔍 Debugging - Looking for Chromium and ChromeDriver...")
        print(f"   Current PATH: {os.environ.get('PATH', 'Not set')}")
        
        # Debug: List what's in common directories
        print("\n📂 Checking common directories:")
        for dir_path in ['/usr/bin', '/usr/local/bin', '/nix/store']:
            if os.path.exists(dir_path):
                print(f"   {dir_path} exists")
                if dir_path == '/nix/store':
                    try:
                        # List directories in nix store
                        items = os.listdir(dir_path)
                        print(f"      Found {len(items)} items in /nix/store")
                        # Look for chromium-related directories
                        chromium_dirs = [item for item in items if 'chromium' in item.lower()]
                        if chromium_dirs:
                            print(f"      Chromium-related directories: {chromium_dirs[:5]}")
                            for chrom_dir in chromium_dirs[:3]:
                                full_path = os.path.join(dir_path, chrom_dir)
                                if os.path.isdir(full_path):
                                    try:
                                        contents = os.listdir(full_path)
                                        print(f"         {chrom_dir}: {contents}")
                                    except:
                                        pass
                    except Exception as e:
                        print(f"      Error listing: {e}")
            else:
                print(f"   {dir_path} does not exist")
        
        print("\n🔍 Searching for binaries...")
        
        # Find Chromium binary
        chromium_binary = None
        
        # Try which first (fastest)
        print("   Trying 'which' command...")
        for cmd in ['chromium', 'chromium-browser', 'google-chrome']:
            result = shutil.which(cmd)
            if result:
                print(f"      Found via 'which {cmd}': {result}")
                chromium_binary = result
                break
        
        # Try glob in nix store
        if not chromium_binary:
            print("   Searching /nix/store with glob...")
            patterns = [
                '/nix/store/*/bin/chromium',
                '/nix/store/*/bin/chromium-browser',
                '/nix/store/*-chromium-*/bin/chromium',
            ]
            for pattern in patterns:
                print(f"      Trying pattern: {pattern}")
                matches = glob_module.glob(pattern)
                if matches:
                    matches.sort()
                    chromium_binary = matches[-1]
                    print(f"      ✅ Found: {chromium_binary}")
                    break
        
        # Try subprocess find (slower but thorough)
        if not chromium_binary:
            print("   Trying 'find' command in /nix/store...")
            try:
                result = subprocess.run(
                    ['find', '/nix/store', '-name', 'chromium', '-type', 'f', '-executable'],
                    capture_output=True, text=True, timeout=10
                )
                if result.stdout.strip():
                    lines = result.stdout.strip().split('\n')
                    print(f"      Found {len(lines)} matches")
                    for line in lines[:5]:
                        print(f"         {line}")
                    chromium_binary = lines[0]
            except Exception as e:
                print(f"      Find command failed: {e}")
        
        if chromium_binary:
            print(f"\n✅ Found Chromium at: {chromium_binary}")
            chrome_options.binary_location = chromium_binary
        else:
            print("\n❌ ERROR: Could not find Chromium binary!")
        
        # Find ChromeDriver
        chromedriver_binary = None
        
        print("\n🔍 Searching for ChromeDriver...")
        
        # Try which first
        result = shutil.which('chromedriver')
        if result:
            print(f"   Found via 'which': {result}")
            chromedriver_binary = result
        
        # Try glob in nix store
        if not chromedriver_binary:
            patterns = [
                '/nix/store/*/bin/chromedriver',
                '/nix/store/*-chromedriver-*/bin/chromedriver',
            ]
            for pattern in patterns:
                matches = glob_module.glob(pattern)
                if matches:
                    matches.sort()
                    chromedriver_binary = matches[-1]
                    print(f"   Found via glob: {chromedriver_binary}")
                    break
        
        # Try find command
        if not chromedriver_binary:
            try:
                result = subprocess.run(
                    ['find', '/nix/store', '-name', 'chromedriver', '-type', 'f', '-executable'],
                    capture_output=True, text=True, timeout=10
                )
                if result.stdout.strip():
                    lines = result.stdout.strip().split('\n')
                    chromedriver_binary = lines[0]
                    print(f"   Found via find: {chromedriver_binary}")
            except:
                pass
        
        if chromedriver_binary:
            print(f"✅ Found ChromeDriver at: {chromedriver_binary}")
        else:
            print("❌ Could not find ChromeDriver")
        
        import warnings
        warnings.filterwarnings('ignore')
        os.environ['WDM_LOG'] = '0'
        
        print("\n🚀 Attempting to start Chrome...")
        
        # Make sure we found chromium
        if not chromium_binary:
            raise Exception("Chromium binary not found. Check nixpacks.toml configuration and logs above.")
        
        try:
            if chromedriver_binary:
                print(f"   Using ChromeDriver at: {chromedriver_binary}")
                service = Service(executable_path=chromedriver_binary)
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                print("   Trying without explicit ChromeDriver path...")
                driver = webdriver.Chrome(options=chrome_options)
            
            print("✅ Chrome started successfully")
            return driver
            
        except Exception as e:
            print(f"❌ Failed to start Chrome: {str(e)}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Could not start Chrome. Error: {str(e)}")
    
    def search_cars(self, keyword: str = "", max_results: int = 20, min_price: int = None, max_price: int = None) -> List[Dict]:
        """Search for cars on Willhaben"""
        driver = None
        try:
            print(f"🔍 Searching: {keyword or 'all cars'}")
            print(f"📊 Max results: {max_results}")
            
            driver = self._create_driver(headless=True)
            
            search_url = f"{self.base_url}/iad/gebrauchtwagen/auto/gebrauchtwagenboerse"
            params = []
            
            if keyword:
                params.append(f"keyword={keyword}")
            if min_price:
                params.append(f"PRICE_FROM={min_price}")
            if max_price:
                params.append(f"PRICE_TO={max_price}")
            params.append(f"rows={min(max_results, 100)}")
            
            if params:
                search_url += "?" + "&".join(params)
            
            print(f"📡 URL: {search_url}")
            driver.get(search_url)
            print("⏳ Waiting for page load...")
            time.sleep(3)
            
            print("📦 Extracting data...")
            page_source = driver.page_source
            json_match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', page_source, re.DOTALL)
            
            if not json_match:
                print("❌ Could not find __NEXT_DATA__ in page")
                return []
            
            print("✅ Found JSON data")
            next_data = json.loads(json_match.group(1))
            page_props = next_data['props']['pageProps']
            search_result = page_props.get('searchResult', {})
            listings = search_result.get('advertSummaryList', {}).get('advertSummary', [])
            
            print(f"✅ Found {len(listings)} listings")
            
            cars = []
            for listing in listings[:max_results]:
                car_data = self._parse_listing_from_json(listing)
                if car_data:
                    cars.append(car_data)
                    print(f"  ✓ {car_data['name'][:40]}...")
            
            print(f"✅ Successfully parsed {len(cars)} cars")
            return cars
            
        except Exception as e:
            print(f"❌ ERROR in search_cars: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
        finally:
            if driver:
                print("🔒 Closing browser...")
                driver.quit()
    
    def _parse_listing_from_json(self, listing: Dict) -> Optional[Dict]:
        """Parse basic listing info from search results"""
        try:
            listing_id = listing.get('id')
            car_data = {
                'listing_id': listing_id,
                'name': listing.get('description', 'N/A'),
                'price': None,
                'url': None,
                'thumbnail': None
            }
            
            # Build URL
            if listing_id:
                contexes = listing.get('contextLinkList', {}).get('contextLink', [])
                for context in contexes:
                    if context.get('id') == 'seoSelfLink':
                        seo_url = context.get('uri', '')
                        match = re.search(r'/(gebrauchtwagen/d/auto/.+)$', seo_url)
                        if match:
                            car_data['url'] = f"{self.base_url}/iad/{match.group(1)}"
                        break
            
            # Get price
            attributes = listing.get('attributes', {}).get('attribute', [])
            for attr in attributes:
                if attr.get('name') == 'PRICE' and attr.get('values'):
                    car_data['price'] = attr['values'][0]
                    break
            
            # Get thumbnail (first image)
            images = listing.get('advertImageList', {}).get('advertImage', [])
            if images:
                first_image = images[0]
                # Use mainImageUrl for best quality, fallback to others
                car_data['thumbnail'] = (
                    first_image.get('mainImageUrl') or 
                    first_image.get('referenceImageUrl') or 
                    first_image.get('thumbnailImageUrl')
                )
            
            return car_data
            
        except Exception as e:
            return None
    
    def get_car_details(self, listing_id: str) -> Dict:
        """
        Get essential car details and ALL images for any listing
        """
        driver = None
        try:
            print(f"🔍 Fetching details for listing: {listing_id}")
            driver = self._create_driver(headless=True)
            
            # Try to fetch the page
            url = f"{self.base_url}/iad/gebrauchtwagen/d/auto/listing-{listing_id}"
            print(f"📡 URL: {url}")
            driver.get(url)
            time.sleep(4)
            
            page_source = driver.page_source
            json_match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', page_source, re.DOTALL)
            
            if not json_match:
                return {'error': 'Could not find listing data', 'listing_id': listing_id}
            
            next_data = json.loads(json_match.group(1))
            page_props = next_data['props']['pageProps']
            advert = page_props.get('advertDetails', {})
            
            # Initialize car data
            car_data = {
                'listing_id': listing_id,
                'url': url,
                'name': advert.get('description', 'N/A'),
                'brand': None,
                'model': None,
                'car_type': None,
                'year': None,
                'mileage': None,
                'price': None,
                'fuel_type': None,
                'power_kw': None,
                'power_ps': None,
                'transmission': None,
                'color': None,
                'doors': None,
                'seats': None,
                'condition': None,
                'address': {
                    'street': None,
                    'postal_code': None,
                    'city': None,
                    'country': None
                },
                'images': [],
                'image_count': 0
            }
            
            # Extract attributes
            attributes = advert.get('attributes', {}).get('attribute', [])
            for attr in attributes:
                name = attr.get('name')
                values = attr.get('values', [])
                value = values[0] if values else None
                
                if name == 'MAKE':
                    car_data['brand'] = value
                elif name == 'MODEL':
                    car_data['model'] = value
                elif name == 'BODYTYPE':
                    car_data['car_type'] = value
                elif name == 'YEAR_MODEL':
                    car_data['year'] = value
                elif name == 'MILEAGE':
                    car_data['mileage'] = f"{value} km"
                elif name == 'MOTOR_PRICE/TOTAL':
                    car_data['price'] = f"€ {value}"
                elif name == 'ENGINE/FUEL':
                    car_data['fuel_type'] = value
                elif name == 'ENGINE/EFFECT':
                    car_data['power_kw'] = f"{value} kW"
                elif name == 'MOTOR_POWER':
                    car_data['power_ps'] = f"{value} PS"
                elif name == 'TRANSMISSION':
                    car_data['transmission'] = value
                elif name == 'EXTERIOR_COLOUR_MAIN':
                    car_data['color'] = value
                elif name == 'NO_OF_DOORS':
                    car_data['doors'] = value
                elif name == 'NO_OF_SEATS':
                    car_data['seats'] = value
                elif name == 'MOTOR_CONDITION':
                    car_data['condition'] = value
            
            # Extract address
            address_details = advert.get('advertAddressDetails', {})
            address_lines = address_details.get('addressLines', {}).get('value', [])
            
            if len(address_lines) >= 1:
                car_data['address']['street'] = address_lines[0]
            if len(address_lines) >= 2:
                car_data['address']['city'] = address_lines[1]
            
            car_data['address']['postal_code'] = address_details.get('postCode')
            car_data['address']['country'] = address_details.get('country')
            
            # Extract ALL images
            print("📸 Extracting all images...")
            image_list = advert.get('advertImageList', {}).get('advertImage', [])
            
            for img in image_list:
                # Get the highest quality image available
                img_url = img.get('mainImageUrl') or img.get('referenceImageUrl') or img.get('thumbnailImageUrl')
                
                if img_url and img_url not in car_data['images']:
                    car_data['images'].append(img_url)
            
            car_data['image_count'] = len(car_data['images'])
            
            print(f"✅ Successfully extracted details")
            print(f"   📸 Images: {car_data['image_count']}")
            print(f"   🚗 Car: {car_data['brand']} {car_data['model']}")
            print(f"   📍 Location: {car_data['address']['postal_code']} {car_data['address']['city']}")
            
            return car_data
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'error': str(e), 'listing_id': listing_id}
        finally:
            if driver:
                driver.quit()


# Initialize scraper
scraper = WillhabenCarScraper()

# API Routes

@app.route('/')
def home():
    """API documentation"""
    return jsonify({
        'name': 'Willhaben Car Scraper API',
        'version': '7.2-debug',
        'description': 'Extracts essential car info and ALL images from any listing',
        'endpoints': {
            '/api/search': {
                'method': 'GET',
                'description': 'Search for cars',
                'parameters': {
                    'keyword': 'Search keyword (e.g., BMW, Audi)',
                    'max_results': 'Maximum results (default: 20, max: 100)',
                    'min_price': 'Minimum price',
                    'max_price': 'Maximum price'
                },
                'example': '/api/search?keyword=Audi&max_results=10'
            },
            '/api/car/<listing_id>': {
                'method': 'GET',
                'description': 'Get car details by listing ID - works for ANY car listing',
                'examples': [
                    '/api/car/1880510138  (BMW X3)',
                    '/api/car/1234567890  (Any other car)',
                    '/api/car/9876543210  (Any listing ID)'
                ]
            },
            '/api/health': {
                'method': 'GET',
                'description': 'Health check'
            }
        },
        'data_extracted': {
            'basic_info': ['listing_id', 'url', 'name', 'brand', 'model', 'car_type', 'year', 'price'],
            'technical': ['mileage', 'fuel_type', 'power_kw', 'power_ps', 'transmission', 'color', 'doors', 'seats', 'condition'],
            'address': ['street', 'postal_code', 'city', 'country'],
            'images': ['ALL images in high quality', 'image_count']
        }
    })

@app.route('/api/search', methods=['GET'])
def search_cars():
    """Search for cars"""
    try:
        keyword = request.args.get('keyword', '')
        max_results = int(request.args.get('max_results', 20))
        min_price = request.args.get('min_price', type=int)
        max_price = request.args.get('max_price', type=int)
        
        if max_results > 100:
            max_results = 100
        
        print(f"\n{'='*60}")
        print(f"🔍 Search Request: {keyword or 'all cars'} (max {max_results})")
        print(f"{'='*60}\n")
        
        results = scraper.search_cars(
            keyword=keyword,
            max_results=max_results,
            min_price=min_price,
            max_price=max_price
        )
        
        if results is None:
            results = []
        
        response = {
            'success': len(results) > 0,
            'count': len(results),
            'results': results
        }
        
        if len(results) == 0:
            response['message'] = 'No results found or error occurred. Check server logs.'
        
        return jsonify(response)
        
    except Exception as e:
        print(f"❌ ERROR in search endpoint: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'An error occurred while searching. Please try again.'
        }), 500

@app.route('/api/car/<listing_id>', methods=['GET'])
def get_car_details(listing_id):
    """Get car details by listing ID - works for ANY car"""
    print(f"\n{'='*60}")
    print(f"📄 Getting details for listing: {listing_id}")
    print(f"{'='*60}\n")
    
    details = scraper.get_car_details(listing_id)
    
    return jsonify({
        'success': True,
        'data': details
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check"""
    return jsonify({
        'status': 'healthy',
        'service': 'Willhaben Car Scraper API',
        'version': '7.2-debug'
    })

if __name__ == '__main__':
    import os
    
    print("=" * 80)
    print("🚗 Willhaben Car Scraper API v7.2-debug")
    print("=" * 80)
    print("\n✨ Features:")
    print("   📋 Essential car information (brand, model, year, price, etc.)")
    print("   📍 Complete address (street, postal code, city, country)")
    print("   📸 ALL images from any listing")
    print("   🔢 Works with ANY listing ID")
    print("\n📋 Endpoints:")
    print("   • /")
    print("   • /api/search?keyword=BMW")
    print("   • /api/car/<any_listing_id>")
    print("\n💡 Examples:")
    print("   curl https://your-api.com/api/car/1880510138")
    print("   curl https://your-api.com/api/search?keyword=Audi&max_results=5")
    print("\n" + "=" * 80 + "\n")
    
    # Use PORT from environment (for deployment) or 5001 for local
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=False, host='0.0.0.0', port=port)