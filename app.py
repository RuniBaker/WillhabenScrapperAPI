from flask import Flask, jsonify, request
import time
import re
from typing import List, Dict, Optional
import json
import asyncio
from playwright.async_api import async_playwright

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

class WillhabenCarScraper:
    """
    Streamlined Willhaben scraper using Playwright - extracts essential car info and ALL images
    """
    def __init__(self):
        self.base_url = "https://www.willhaben.at"
    
    async def _create_browser(self):
        """Create and configure Playwright browser"""
        print("🚀 Starting Playwright browser...")
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
            ]
        )
        return playwright, browser
    
    async def _search_cars_async(self, keyword: str = "", max_results: int = 20, min_price: int = None, max_price: int = None) -> List[Dict]:
        """Search for cars on Willhaben (async)"""
        playwright = None
        browser = None
        try:
            print(f"🔍 Searching: {keyword or 'all cars'}")
            print(f"📊 Max results: {max_results}")
            
            playwright, browser = await self._create_browser()
            page = await browser.new_page()
            
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
            await page.goto(search_url, wait_until='networkidle')
            print("⏳ Waiting for page load...")
            await asyncio.sleep(3)
            
            print("📦 Extracting data...")
            page_content = await page.content()
            json_match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', page_content, re.DOTALL)
            
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
            if browser:
                print("🔒 Closing browser...")
                await browser.close()
            if playwright:
                await playwright.stop()
    
    def search_cars(self, keyword: str = "", max_results: int = 20, min_price: int = None, max_price: int = None) -> List[Dict]:
        """Search for cars on Willhaben (sync wrapper)"""
        return asyncio.run(self._search_cars_async(keyword, max_results, min_price, max_price))
    
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
                car_data['thumbnail'] = (
                    first_image.get('mainImageUrl') or 
                    first_image.get('referenceImageUrl') or 
                    first_image.get('thumbnailImageUrl')
                )
            
            return car_data
            
        except Exception as e:
            return None
    
    async def _get_car_details_async(self, listing_id: str) -> Dict:
        """Get essential car details and ALL images for any listing (async)"""
        playwright = None
        browser = None
        try:
            print(f"🔍 Fetching details for listing: {listing_id}")
            playwright, browser = await self._create_browser()
            page = await browser.new_page()
            
            url = f"{self.base_url}/iad/gebrauchtwagen/d/auto/listing-{listing_id}"
            print(f"📡 URL: {url}")
            await page.goto(url, wait_until='networkidle')
            await asyncio.sleep(4)
            
            page_content = await page.content()
            json_match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', page_content, re.DOTALL)
            
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
            if browser:
                await browser.close()
            if playwright:
                await playwright.stop()
    
    def get_car_details(self, listing_id: str) -> Dict:
        """Get car details (sync wrapper)"""
        return asyncio.run(self._get_car_details_async(listing_id))


# Initialize scraper
scraper = WillhabenCarScraper()

# API Routes

@app.route('/')
def home():
    """API documentation"""
    return jsonify({
        'name': 'Willhaben Car Scraper API',
        'version': '8.0 (Playwright)',
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
        'version': '8.0 (Playwright)'
    })

if __name__ == '__main__':
    import os
    
    print("=" * 80)
    print("🚗 Willhaben Car Scraper API v8.0 (Playwright)")
    print("=" * 80)
    print("\n✨ Features:")
    print("   📋 Essential car information (brand, model, year, price, etc.)")
    print("   📍 Complete address (street, postal code, city, country)")
    print("   📸 ALL images from any listing")
    print("   🔢 Works with ANY listing ID")
    print("   🎭 Powered by Playwright (easier deployment)")
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