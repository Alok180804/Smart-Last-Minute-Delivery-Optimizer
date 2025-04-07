import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openrouteservice import Client
from geopy.distance import geodesic
from datetime import datetime, timedelta
import time 
import os

ORS_API_KEY = os.environ["ORS_API_KEY"]


# -------------------------- CONFIG --------------------------
SERVICE_ACCOUNT_FILE = "service_account.json"
SHEET_NAME = "delivery_orders"
darkstore_coords = (12.9093, 77.6483)
max_cluster_distance_m = 300

# -------------------------- Setup --------------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1
ors = Client(key=ORS_API_KEY)

# -------------------------- Delivery Partners --------------------------
delivery_partners = [
    {"id": i + 1, "available": True, "free_at": None, "current_location": darkstore_coords}
    for i in range(18)
]

# -------------------------- Helper Functions --------------------------
def get_orders():
    data = sheet.get_all_values()
    headers = data[0]
    orders = data[1:]
    parsed = []
    for i, row in enumerate(orders):
        try:
            parsed.append({
                "row_index": i + 2,
                "order_id": row[0],
                "timestamp": row[1],
                "lat": float(row[2]),
                "lng": float(row[3]),
                "item_count": int(row[4]),
                "status": row[5].lower(),
                "assigned_partner": row[6],
                "eta": int(row[7]) if row[7] else None,
                "return_eta": int(row[8]) if row[8] else None
            })
        except:
            continue
    return parsed

def get_unassigned_orders(orders):
    return sorted([o for o in orders if o["status"] == "unassigned"], key=lambda x: x["timestamp"] or "")

def get_available_partner():
    now = datetime.now()
    for partner in delivery_partners:
        if not partner["available"] and partner["free_at"] and now >= partner["free_at"]:
            partner["available"] = True
        if partner["available"]:
            return partner
    return None


def assign_orders_to_partner(orders):
    now = datetime.now()
    partner = get_available_partner()
    if not partner:
        print("❌ No available delivery partners.")
        return

    coords = [(o["lng"], o["lat"]) for o in orders]
    try:
        route = ors.directions(
            coordinates=[(darkstore_coords[1], darkstore_coords[0])] + coords + [(darkstore_coords[1], darkstore_coords[0])],
            profile='driving-car',
            format='geojson'
        )
    except Exception as e:
        print(f"❌ ORS routing failed: {e}")
        return

    total_duration_sec = route["features"][0]["properties"]["summary"]["duration"]
    eta_min = round(total_duration_sec / 60)
    return_eta_min = round(eta_min * 0.8)
    free_time = now + timedelta(minutes=eta_min + return_eta_min)

    for order in orders:
        row = order["row_index"]
        delivery_time = now + timedelta(minutes=eta_min)
        return_time = delivery_time + timedelta(minutes=return_eta_min)

        sheet.update_cell(row, 6, "in_transit")
        sheet.update_cell(row, 7, f"Partner {partner['id']}")
        sheet.update_cell(row, 8, eta_min)
        sheet.update_cell(row, 9, return_eta_min)
        sheet.update_cell(row, 10, delivery_time.strftime("%Y-%m-%d %H:%M:%S"))
        sheet.update_cell(row, 11, return_time.strftime("%Y-%m-%d %H:%M:%S"))

        if not order["timestamp"]:
            sheet.update_cell(row, 2, now.strftime("%Y-%m-%d %H:%M:%S"))

    partner["available"] = False
    partner["free_at"] = free_time
    print(f"✅ Assigned orders {[o['order_id'] for o in orders]} to Partner {partner['id']}")

def update_delivery_status(orders):
    now = datetime.now()
    for order in orders:
        if order["status"] == "in_transit" and order["eta"]:
            delivery_time = datetime.strptime(order["timestamp"], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=order["eta"])
            return_time = delivery_time + timedelta(minutes=order["return_eta"])
            if now >= delivery_time and now < return_time:
                row = order["row_index"]
                sheet.update_cell(row, 6, "delivered")
                print(f"📦 Order {order['order_id']} marked as delivered.")

# -------------------------- Main Loop --------------------------
def assign_orders():
    print("🚀 Smart Order Assignment Engine Started...")

    while True:
        try:
            orders = get_orders()
            update_delivery_status(orders)

            unassigned = get_unassigned_orders(orders)

            if len(unassigned) == 0:
                print("⏳ No unassigned orders. Waiting...")

            elif len(unassigned) == 1:
                print("🕒 Only 1 unassigned order. Waiting for second...")

            else:
                first = unassigned[0]
                second = unassigned[1]

                dist = geodesic((first["lat"], first["lng"]), (second["lat"], second["lng"]))

                if dist.meters <= max_cluster_distance_m:
                    print(f"📍 Orders {first['order_id']} & {second['order_id']} within {dist.meters:.2f}m. Assigning together.")
                    assign_orders_to_partner([first, second])
                else:
                    print(f"📍 Orders too far apart ({dist.meters:.2f}m). Assigning order {first['order_id']} solo.")
                    assign_orders_to_partner([first])

            time.sleep(30)

        except Exception as e:
            print(f"❌ Error in main loop: {e}")
            time.sleep(10)

assign_orders()
