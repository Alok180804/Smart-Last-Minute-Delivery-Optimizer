import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import random
import math
from openrouteservice import Client 
import os

ORS_API_KEY = os.environ["ORS_API_KEY"]
PRIVATE_KEY = os.environ["GOOGLE_PRIVATE_KEY"].replace('\\n', '\n')


# --------------------------
# CONFIG
# --------------------------
SHEET_NAME = "delivery_orders"
SERVICE_ACCOUNT_FILE = "service_account.json"

# --------------------------
# Setup
# --------------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1

ors = Client(key=ORS_API_KEY)

# --------------------------
# Dark Store Coordinates (HSR Layout)
# --------------------------
darkstore_lat = 12.9093
darkstore_lng = 77.6483

# --------------------------
# Helper Functions
# --------------------------
def is_routable(lat, lng):
    try:
        result = ors.pelias_reverse((lng, lat))  # ORS expects (lng, lat)
        return result and len(result['features']) > 0
    except Exception as e:
        print(f"Routing check failed for ({lat}, {lng}): {e}")
        return False

def generate_valid_random_location(center_lat, center_lng, radius_km=2.5):
    for _ in range(10):  # Try up to 10 times
        lat_offset = random.uniform(-radius_km / 111, radius_km / 111)
        lng_offset = random.uniform(
            -radius_km / (111 * abs(math.cos(math.radians(center_lat)))),
            radius_km / (111 * abs(math.cos(math.radians(center_lat))))
        )
        lat = round(center_lat + lat_offset, 6)
        lng = round(center_lng + lng_offset, 6)
        if is_routable(lat, lng):
            return lat, lng
    return None, None

def simulate_order(order_id):
    lat, lng = generate_valid_random_location(darkstore_lat, darkstore_lng)
    if lat is None or lng is None:
        return None
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    item_count = random.randint(1, 5)
    status = "unassigned"
    return [order_id, timestamp, lat, lng, item_count, status]

# --------------------------
# Main Loop - 1 Order/Minute
# --------------------------
order_id = 1

print("Starting Quick Commerce Order Simulation...")

while True:
    try:
        order_data = simulate_order(order_id)
        if order_data:
            sheet.append_row(order_data)
            print(f"Order {order_id} added at {order_data[1]}: {order_data}")
        else:
            print(f"Order {order_id} skipped: Unable to find routable location.")
        order_id += 1
        time.sleep(60)
    except Exception as e:
        print(f" Error: {e}")
        time.sleep(10)
